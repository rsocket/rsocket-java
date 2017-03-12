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

import io.reactivesocket.Frame.Lease;
import io.reactivesocket.Frame.Request;
import io.reactivesocket.events.EventListener;
import io.reactivesocket.exceptions.ApplicationException;
import io.reactivesocket.frame.FrameHeaderFlyweight;
import io.reactivesocket.internal.DisabledEventPublisher;
import io.reactivesocket.internal.EventPublisher;
import io.reactivesocket.internal.KnownErrorFilter;
import io.reactivesocket.internal.LimitableRequestPublisher;
import io.reactivesocket.lease.LeaseEnforcingSocket;
import org.agrona.collections.Int2ObjectHashMap;
import org.reactivestreams.Publisher;
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

    private final Int2ObjectHashMap<Subscription> sendingSubscriptions;
    private final Int2ObjectHashMap<UnicastProcessor<Payload>> receivers;

    private final ReactiveSocket requestHandler;

    private volatile Disposable subscribe;

    public ServerReactiveSocket(DuplexConnection connection, ReactiveSocket requestHandler,
                                 boolean clientHonorsLease, Consumer<Throwable> errorConsumer,
                                 EventPublisher<? extends EventListener> eventPublisher) {
        this.requestHandler = requestHandler;
        this.connection = connection;
        this.errorConsumer = new KnownErrorFilter(errorConsumer);
        this.sendingSubscriptions = new Int2ObjectHashMap<>();
        this.receivers = new Int2ObjectHashMap<>();

        connection.onClose()
            .doFinally(signalType -> cleanup())
            .subscribe();
        if (requestHandler instanceof LeaseEnforcingSocket) {
            LeaseEnforcingSocket enforcer = (LeaseEnforcingSocket) requestHandler;
            enforcer.acceptLeaseSender(lease -> {
                if (!clientHonorsLease) {
                    return;
                }
                Frame leaseFrame = Lease.from(lease.getTtl(), lease.getAllowedRequests(), lease.metadata());
                connection.sendOne(leaseFrame)
                    .doOnError(errorConsumer)
                    .subscribe();
            });
        }
    }

    public ServerReactiveSocket(DuplexConnection connection, ReactiveSocket requestHandler,
                                 boolean clientHonorsLease, Consumer<Throwable> errorConsumer) {
        this(connection, requestHandler, clientHonorsLease, errorConsumer, new DisabledEventPublisher<>());
    }

    public ServerReactiveSocket(DuplexConnection connection, ReactiveSocket requestHandler,
                                 Consumer<Throwable> errorConsumer) {
        this(connection, requestHandler, true, errorConsumer);
    }

    @Override
    public Mono<Void> fireAndForget(Payload payload) {
        return requestHandler.fireAndForget(payload);
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
        return requestHandler.requestResponse(payload);
    }

    @Override
    public Flux<Payload> requestStream(Payload payload) {
        return requestHandler.requestStream(payload);
    }

    @Override
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
        return requestHandler.requestChannel(payloads);
    }

    @Override
    public Mono<Void> metadataPush(Payload payload) {
        return requestHandler.metadataPush(payload);
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
                int streamId = frame.getStreamId();
                UnicastProcessor<Payload> receiver;
                switch (frame.getType()) {
                    case FIRE_AND_FORGET:
                        return handleFireAndForget(streamId, fireAndForget(frame));
                    case REQUEST_RESPONSE:
                        return handleRequestResponse(streamId, requestResponse(frame));
                    case CANCEL:
                        return handleCancelFrame(streamId);
                    case KEEPALIVE:
                        return handleKeepAliveFrame(frame);
                    case REQUEST_N:
                        return handleRequestN(streamId, frame);
                    case REQUEST_STREAM:
                        return handleStream(streamId, requestStream(frame));
                    case REQUEST_CHANNEL:
                        return handleChannel(streamId, frame);
                    case PAYLOAD:
                        // TODO: Hook in receiving socket.
                        return Mono.empty();
                    case METADATA_PUSH:
                        return metadataPush(frame);
                    case LEASE:
                        // Lease must not be received here as this is the server end of the socket which sends leases.
                        return Mono.empty();
                    case NEXT:
                        synchronized (ServerReactiveSocket.this) {
                            receiver = receivers.get(streamId);
                        }
                        if (receiver != null) {
                            receiver.onNext(frame);
                        }
                        return Mono.empty();
                    case COMPLETE:
                        synchronized (ServerReactiveSocket.this) {
                            receiver = receivers.get(streamId);
                        }
                        if (receiver != null) {
                            receiver.onComplete();
                        }
                        return Mono.empty();
                    case ERROR:
                        synchronized (ServerReactiveSocket.this) {
                            receiver = receivers.get(streamId);
                        }
                        if (receiver != null) {
                            receiver.onError(new ApplicationException(frame));
                        }
                        return Mono.empty();
                    case NEXT_COMPLETE:
                        synchronized (ServerReactiveSocket.this) {
                            receiver = receivers.get(streamId);
                        }
                        if (receiver != null) {
                            receiver.onNext(frame);
                            receiver.onComplete();
                        }

                        return Mono.empty();

                    case SETUP:
                        return handleError(streamId, new IllegalStateException("Setup frame received post setup."));
                    default:
                        return handleError(streamId, new IllegalStateException("ServerReactiveSocket: Unexpected frame type: "
                            + frame.getType()));
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

    private synchronized void cleanup() {
        subscribe.dispose();
        sendingSubscriptions.values().forEach(Subscription::cancel);
        sendingSubscriptions.clear();
        receivers.values().forEach(Subscription::cancel);
        sendingSubscriptions.clear();
        requestHandler.close().subscribe();
    }

    private Mono<Void> handleFireAndForget(int streamId, Mono<Void> result) {
        return result
            .doOnSubscribe(subscription -> addSubscription(streamId, subscription))
            .doOnError(t -> {
                removeSubscription(streamId);
                errorConsumer.accept(t);
            })
            .doFinally(signalType -> removeSubscription(streamId))
            .ignoreElement();
    }

    private Mono<Void> send(int streamId, Publisher<Frame> responseFrames) {
        return Mono.create(sink ->
            connection
                .send(responseFrames)
                .doOnCancel(() -> {
                    if (connection.availability() > 0.0) {
                        connection.sendOne(Frame.Cancel.from(streamId)).subscribe(null, errorConsumer::accept);
                    }
                })
                .doOnError(t -> {
                    if (connection.availability() > 0.0) {
                        connection.sendOne(Frame.Error.from(streamId, t)).subscribe(null, errorConsumer::accept);
                    }
                })
                .doFinally(signalType -> {
                    sink.success();
                    removeSubscription(streamId);
                })
                .subscribe()
        );
    }

    private Mono<Void> handleRequestResponse(int streamId, Mono<Payload> response) {
        Mono<Frame> responseFrame = response
            .doOnSubscribe(subscription -> addSubscription(streamId, subscription))
            .map(payload ->
                Frame.PayloadFrame.from(streamId, FrameType.NEXT_COMPLETE, payload.getMetadata(), payload.getData(), FrameHeaderFlyweight.FLAGS_C));

        return send(streamId, responseFrame);
    }

    private Mono<Void> handleStream(int streamId, Flux<Payload> response) {
        return handleStream(streamId, response, 1);
    }

    private Mono<Void> handleStream(int streamId, Flux<Payload> response, int initialRequestN) {
        Flux<Frame> responseFrames = response
            .map(payload -> Frame.PayloadFrame.from(streamId, FrameType.NEXT, payload))
            .transform(f -> {
                LimitableRequestPublisher<Frame> wrap = LimitableRequestPublisher.wrap(f);
                synchronized (ServerReactiveSocket.this) {
                    wrap.increaseRequestLimit(initialRequestN);
                    sendingSubscriptions.put(streamId, wrap);
                }

                return wrap;
            });

        return send(streamId, responseFrames);
    }

    private Mono<Void> handleChannel(int streamId, Frame firstFrame) {
        return Mono.defer(() -> {
            UnicastProcessor<Frame> frames = UnicastProcessor.create();
            int initialRequestN = Request.initialRequestN(firstFrame);

            Flux<Payload> payloads = frames
                .doOnCancel(() -> {
                    if (connection.availability() > 0.0) {
                        connection.sendOne(Frame.Cancel.from(streamId)).subscribe(null, errorConsumer::accept);
                    }
                })
                .doOnError(t -> {
                    if (connection.availability() > 0.0) {
                        connection.sendOne(Frame.Error.from(streamId, t)).doOnError(throwable -> System.out.println("EREREERERERERER")).subscribe(null, errorConsumer::accept);
                    }
                })
                .doOnRequest(l -> {
                    if (connection.availability() > 0.0) {
                        connection.sendOne(Frame.RequestN.from(streamId, l)).subscribe(null, errorConsumer::accept);
                    }
                })
                .cast(Payload.class);

            Flux<Payload> responses = requestChannel(payloads);

            return handleStream(streamId, responses, initialRequestN)
                .doFinally(s -> {
                    synchronized (ServerReactiveSocket.this) {
                        receivers.remove(streamId);
                    }
                });
        });
    }

    private Mono<Void> handleKeepAliveFrame(Frame frame) {
        if (Frame.Keepalive.hasRespondFlag(frame)) {
            return connection.sendOne(Frame.Keepalive.from(Frame.NULL_BYTEBUFFER, false))
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
        Subscription subscription;
        synchronized (this) {
            subscription = sendingSubscriptions.get(streamId);
        }
        if (subscription != null) {
            int n = Frame.RequestN.requestN(frame);
            subscription.request(n >= Integer.MAX_VALUE ? Long.MAX_VALUE : n);
        }
        return Mono.empty();
    }

    private synchronized void addSubscription(int streamId, Subscription subscription) {
        sendingSubscriptions.put(streamId, subscription);
    }

    private synchronized void removeSubscription(int streamId) {
        sendingSubscriptions.remove(streamId);
    }

}
