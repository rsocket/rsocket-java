/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.reactivesocket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.collection.IntObjectHashMap;
import io.reactivesocket.Frame.Lease;
import io.reactivesocket.Frame.Request;
import io.reactivesocket.exceptions.ApplicationException;
import io.reactivesocket.frame.FrameHeaderFlyweight;
import io.reactivesocket.internal.KnownErrorFilter;
import io.reactivesocket.internal.LimitableRequestPublisher;
import io.reactivesocket.lease.LeaseEnforcingSocket;
import io.reactivesocket.util.PayloadImpl;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;

import java.util.Collection;
import java.util.function.Consumer;

/**
 * Server side ReactiveSocket. Receives {@link Frame}s from a
 * {@link ClientReactiveSocket}
 */
public class ServerReactiveSocket implements ReactiveSocket {

    private final DuplexConnection connection;
    private final Consumer<Throwable> errorConsumer;

    private final IntObjectHashMap<Subscription> sendingSubscriptions;
    private final IntObjectHashMap<UnicastProcessor<Payload>> channelProcessors;

    private final ReactiveSocket requestHandler;

    private volatile Disposable subscribe;

    public ServerReactiveSocket(DuplexConnection connection, ReactiveSocket requestHandler,
                                boolean clientHonorsLease, Consumer<Throwable> errorConsumer) {
        this.requestHandler = requestHandler;
        this.connection = connection;
        this.errorConsumer = new KnownErrorFilter(errorConsumer);
        this.sendingSubscriptions = new IntObjectHashMap<>();
        this.channelProcessors = new IntObjectHashMap<>();

        connection.onClose()
            .doFinally(signalType -> cleanup())
            .subscribe();
        if (requestHandler instanceof LeaseEnforcingSocket) {
            LeaseEnforcingSocket enforcer = (LeaseEnforcingSocket) requestHandler;
            enforcer.acceptLeaseSender(lease -> {
                if (!clientHonorsLease) {
                    return;
                }
                ByteBuf metadata = lease.getMetadata() == null ? Unpooled.EMPTY_BUFFER : Unpooled.wrappedBuffer(lease.getMetadata());
                Frame leaseFrame = Lease.from(lease.getTtl(), lease.getAllowedRequests(), metadata);
                connection.sendOne(leaseFrame)
                    .doOnError(errorConsumer)
                    .subscribe();
            });
        }
    }

    public ServerReactiveSocket(DuplexConnection connection, ReactiveSocket requestHandler,
                                Consumer<Throwable> errorConsumer) {
        this(connection, requestHandler, true, errorConsumer);
    }

    @Override
    public Mono<Void> fireAndForget(Payload payload) {
        try {
            return requestHandler.fireAndForget(payload);
        } catch (Throwable t) {
            return Mono.error(t);
        }
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
        try {
            return requestHandler.requestResponse(payload);
        } catch (Throwable t) {
            return Mono.error(t);
        }
    }

    @Override
    public Flux<Payload> requestStream(Payload payload) {
        try {
            return requestHandler.requestStream(payload);
        } catch (Throwable t) {
            return Flux.error(t);
        }
    }

    @Override
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
        try {
            return requestHandler.requestChannel(payloads);
        } catch (Throwable t) {
            return Flux.error(t);
        }
    }

    @Override
    public Mono<Void> metadataPush(Payload payload) {
        try {
            return requestHandler.metadataPush(payload);
        } catch (Throwable t) {
            return Mono.error(t);
        }
    }

    @Override
    public Mono<Void> close() {
        if (subscribe != null) {
            subscribe.dispose();
        }

        return connection.close();
    }

    @Override
    public Mono<Void> onClose() {
        return connection.onClose();
    }

    public ServerReactiveSocket start() {
        subscribe = connection
            .receive()
            .flatMap(frame -> {
                try {
                    int streamId = frame.getStreamId();
                    Subscriber<Payload> receiver;
                    switch (frame.getType()) {
                        case FIRE_AND_FORGET:
                            return handleFireAndForget(streamId, fireAndForget(new PayloadImpl(frame)));
                        case REQUEST_RESPONSE:
                            return handleRequestResponse(streamId, requestResponse(new PayloadImpl(frame)));
                        case CANCEL:
                            return handleCancelFrame(streamId);
                        case KEEPALIVE:
                            return handleKeepAliveFrame(frame);
                        case REQUEST_N:
                            return handleRequestN(streamId, frame);
                        case REQUEST_STREAM:
                            return handleStream(streamId, requestStream(new PayloadImpl(frame)), frame);
                        case REQUEST_CHANNEL:
                            return handleChannel(streamId, frame);
                        case PAYLOAD:
                            // TODO: Hook in receiving socket.
                            return Mono.empty();
                        case METADATA_PUSH:
                            return metadataPush(new PayloadImpl(frame));
                        case LEASE:
                            // Lease must not be received here as this is the server end of the socket which sends leases.
                            return Mono.empty();
                        case NEXT:
                            receiver = getChannelProcessor(streamId);
                            if (receiver != null) {
                                receiver.onNext(new PayloadImpl(frame));
                            }
                            return Mono.empty();
                        case COMPLETE:
                            receiver = getChannelProcessor(streamId);
                            if (receiver != null) {
                                receiver.onComplete();
                            }
                            return Mono.empty();
                        case ERROR:
                            receiver = getChannelProcessor(streamId);
                            if (receiver != null) {
                                receiver.onError(new ApplicationException(new PayloadImpl(frame)));
                            }
                            return Mono.empty();
                        case NEXT_COMPLETE:
                            receiver = getChannelProcessor(streamId);
                            if (receiver != null) {
                                receiver.onNext(new PayloadImpl(frame));
                                receiver.onComplete();
                            }

                            return Mono.empty();

                        case SETUP:
                            return handleError(streamId, new IllegalStateException("Setup frame received post setup."));
                        default:
                            return handleError(streamId, new IllegalStateException("ServerReactiveSocket: Unexpected frame type: "
                                    + frame.getType()));
                    }
                } finally {
                    frame.release();
                }
            })
            .doOnError(t -> {
                errorConsumer.accept(t);

                //TODO: This should be error?

                Collection<Subscription> values;
                synchronized (this) {
                    values = sendingSubscriptions.values();
                }
                values
                    .forEach(Subscription::cancel);
            })
            .subscribe();
        return this;
    }

    private void cleanup() {
        if (subscribe != null) {
            subscribe.dispose();
        }

        cleanUpSendingSubscriptions();
        cleanUpChannelProcessors();

        requestHandler.close().subscribe();
    }

    private synchronized void cleanUpSendingSubscriptions() {
        sendingSubscriptions.values().forEach(Subscription::cancel);
        sendingSubscriptions.clear();
    }

    private synchronized void cleanUpChannelProcessors() {
        channelProcessors.values().forEach(Subscription::cancel);
        channelProcessors.clear();
    }

    private Mono<Void> handleFireAndForget(int streamId, Mono<Void> result) {
        return result
            .doOnSubscribe(subscription -> addSubscription(streamId, subscription))
            .doOnError(errorConsumer)
            .doFinally(signalType -> removeSubscription(streamId))
            .ignoreElement();
    }

    private Mono<Void> handleRequestResponse(int streamId, Mono<Payload> response) {
        Mono<Frame> responseFrame =
            response
                .doOnSubscribe(subscription -> addSubscription(streamId, subscription))
                .map(payload ->
                    Frame.PayloadFrame.from(streamId, FrameType.NEXT_COMPLETE, payload, FrameHeaderFlyweight.FLAGS_C))
                .doOnError(t -> {
                    if (connection.availability() > 0.0) {
                        connection.sendOne(Frame.Error.from(streamId, t)).subscribe(null, errorConsumer);
                    }
                })
                .doFinally(signalType -> {
                    removeSubscription(streamId);
                });

        return responseFrame.then(connection::sendOne);
    }

    private Mono<Void> handleStream(int streamId, Flux<Payload> response, Frame firstFrame) {
        int initialRequestN = Request.initialRequestN(firstFrame);
        Flux<Frame> responseFrames =
            response
                .map(payload -> Frame.PayloadFrame.from(streamId, FrameType.NEXT, payload))
                .transform(frameFlux -> {
                    LimitableRequestPublisher<Frame> frames = LimitableRequestPublisher.wrap(frameFlux);
                    synchronized (this) {
                        frames.increaseRequestLimit(initialRequestN);
                        sendingSubscriptions.put(streamId, frames);
                    }

                    return frames;
                })
                .doOnCancel(() -> {
                    if (connection.availability() > 0.0) {
                        connection.sendOne(Frame.Cancel.from(streamId)).subscribe(null, errorConsumer);
                    }
                })
                .doOnError(t -> {
                    if (connection.availability() > 0.0) {
                        connection.sendOne(Frame.Error.from(streamId, t)).subscribe(null, errorConsumer);
                    }
                })
                .doOnComplete(() -> {
                    if (connection.availability() > 0.0) {
                        connection.sendOne(Frame.PayloadFrame.from(streamId, FrameType.COMPLETE)).subscribe(null, errorConsumer);
                    }
                })
                .doFinally(signalType -> {
                    removeSubscription(streamId);
                });

        return connection.send(responseFrames);
    }

    private Mono<Void> handleChannel(int streamId, Frame firstFrame) {
        UnicastProcessor<Payload> frames = UnicastProcessor.create();
        addChannelProcessor(streamId, frames);

        Flux<Payload> payloads = frames
            .doOnCancel(() -> {
                if (connection.availability() > 0.0) {
                    connection.sendOne(Frame.Cancel.from(streamId)).subscribe(null, errorConsumer);
                }
            })
            .doOnError(t -> {
                if (connection.availability() > 0.0) {
                    connection.sendOne(Frame.Error.from(streamId, t)).subscribe(null, errorConsumer);
                }
            })
            .doOnRequest(l -> {
                if (connection.availability() > 0.0) {
                    connection.sendOne(Frame.RequestN.from(streamId, l)).subscribe(null, errorConsumer);
                }
            })
            .doFinally(signalType -> {
                removeChannelProcessor(streamId);
            });

        return handleStream(streamId, requestChannel(payloads), firstFrame);
    }

    private Mono<Void> handleKeepAliveFrame(Frame frame) {
        if (Frame.Keepalive.hasRespondFlag(frame)) {
            return connection.sendOne(Frame.Keepalive.from(Unpooled.EMPTY_BUFFER, false))
                .doOnError(errorConsumer);
        }
        return Mono.empty();
    }

    private Mono<Void> handleCancelFrame(int streamId) {
        Subscription subscription;
        synchronized (this) {
            subscription = sendingSubscriptions.remove(streamId);
        }

        if (subscription != null) {
            subscription.cancel();
        }

        return Mono.empty();
    }

    private Mono<Void> handleError(int streamId, Throwable t) {
        errorConsumer.accept(t);
        return connection
            .sendOne(Frame.Error.from(streamId, t))
            .doOnError(errorConsumer);
    }

    private Mono<Void> handleRequestN(int streamId, Frame frame) {
        final Subscription subscription = getSubscription(streamId);
        if (subscription != null) {
            int n = Frame.RequestN.requestN(frame);
            subscription.request(n >= Integer.MAX_VALUE ? Long.MAX_VALUE : n);
        }
        return Mono.empty();
    }

    private synchronized void addSubscription(int streamId, Subscription subscription) {
        sendingSubscriptions.put(streamId, subscription);
    }

    private synchronized Subscription getSubscription(int streamId) {
        return sendingSubscriptions.get(streamId);
    }

    private synchronized void removeSubscription(int streamId) {
        sendingSubscriptions.remove(streamId);
    }

    private synchronized void addChannelProcessor(int streamId, UnicastProcessor<Payload> processor) {
        channelProcessors.put(streamId, processor);
    }

    private synchronized UnicastProcessor<Payload> getChannelProcessor(int streamId) {
        return channelProcessors.get(streamId);
    }

    private synchronized void removeChannelProcessor(int streamId) {
        channelProcessors.remove(streamId);
    }
}