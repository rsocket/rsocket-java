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

import io.reactivesocket.client.KeepAliveProvider;
import io.reactivesocket.events.EventListener;
import io.reactivesocket.exceptions.Exceptions;
import io.reactivesocket.internal.DisabledEventPublisher;
import io.reactivesocket.internal.EventPublisher;
import io.reactivesocket.internal.KnownErrorFilter;
import io.reactivesocket.internal.LimitableRequestPublisher;
import io.reactivesocket.lease.Lease;
import io.reactivesocket.lease.LeaseImpl;
import org.agrona.collections.Int2ObjectHashMap;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

/**
 * Client Side of a ReactiveSocket socket. Sends {@link Frame}s
 * to a {@link ServerReactiveSocket}
 */
public class ClientReactiveSocket implements ReactiveSocket {

    private final DuplexConnection connection;
    private final Consumer<Throwable> errorConsumer;
    private final StreamIdSupplier streamIdSupplier;
    private final KeepAliveProvider keepAliveProvider;
    private final MonoProcessor<Void> started;
    private final Int2ObjectHashMap<LimitableRequestPublisher> senders;
    private final Int2ObjectHashMap<Subscriber<? super Frame>> receivers;

    private Disposable keepAliveSendSub;
    private volatile Consumer<Lease> leaseConsumer; // Provided on start()

    public ClientReactiveSocket(DuplexConnection connection, Consumer<Throwable> errorConsumer,
                                StreamIdSupplier streamIdSupplier, KeepAliveProvider keepAliveProvider,
                                EventPublisher<? extends EventListener> publisher) {
        this.connection = connection;
        this.errorConsumer = new KnownErrorFilter(errorConsumer);
        this.streamIdSupplier = streamIdSupplier;
        this.keepAliveProvider = keepAliveProvider;
        this.started = MonoProcessor.create();

        senders = new Int2ObjectHashMap<>(256, 0.9f);
        receivers = new Int2ObjectHashMap<>(256, 0.9f);
        connection.onClose()
            .doFinally(signalType -> cleanup())
            .subscribe();
    }

    public ClientReactiveSocket(DuplexConnection connection, Consumer<Throwable> errorConsumer,
                                StreamIdSupplier streamIdSupplier, KeepAliveProvider keepAliveProvider) {
        this(connection, errorConsumer, streamIdSupplier, keepAliveProvider, new DisabledEventPublisher<>());
    }

    @Override
    public Mono<Void> fireAndForget(Payload payload) {
        Mono<Void> defer = Mono.defer(() -> {
            final int streamId = nextStreamId();
            final Frame requestFrame = Frame.Request.from(streamId, FrameType.FIRE_AND_FORGET, payload, 0);
            return connection.sendOne(requestFrame);
        });

        return started.then(defer);
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
        return handleRequestResponse(payload);
    }

    @Override
    public Flux<Payload> requestStream(Payload payload) {
        return handleStreamResponse(Flux.just(payload), FrameType.REQUEST_STREAM);
    }

    @Override
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
        return handleStreamResponse(Flux.from(payloads), FrameType.REQUEST_CHANNEL);
    }

    @Override
    public Mono<Void> metadataPush(Payload payload) {
        final Frame requestFrame = Frame.Request.from(0, FrameType.METADATA_PUSH, payload, 0);
        return connection.sendOne(requestFrame);
    }

    @Override
    public double availability() {
        return connection.availability();
    }

    @Override
    public Mono<Void> close() {
        return connection.close();
    }

    @Override
    public Mono<Void> onClose() {
        return connection.onClose();
    }

    public ClientReactiveSocket start(Consumer<Lease> leaseConsumer) {
        this.leaseConsumer = leaseConsumer;

        keepAliveSendSub = connection.send(keepAliveProvider.ticks()
            .map(i -> Frame.Keepalive.from(Frame.NULL_BYTEBUFFER, true)))
            .subscribe(null, errorConsumer);

        connection
            .receive()
            .doOnSubscribe(subscription -> started.onComplete())
            .doOnNext(this::handleIncomingFrames)
            .doOnError(errorConsumer)
            .subscribe();

        return this;
    }

    private Mono<Payload> handleRequestResponse(final Payload payload) {
        return started.then(() -> {
            int streamId = nextStreamId();
            final Frame requestFrame = Frame.Request.from(streamId, FrameType.REQUEST_RESPONSE, payload, 1);

            MonoProcessor<Payload> receiver = MonoProcessor.create();

            synchronized (this) {
                receivers.put(streamId, receiver);
            }

            MonoProcessor<Void> subscribedRequest = connection
                .sendOne(requestFrame)
                .doOnError(t -> {
                    errorConsumer.accept(t);
                    receiver.cancel();
                })
                .subscribe();

            return receiver
                .doOnError(t -> {
                    if (contains(streamId) && connection.availability() > 0.0) {
                        connection
                            .sendOne(Frame.Error.from(streamId, t))
                            .doOnError(errorConsumer::accept)
                            .subscribe();
                    }
                })
                .doOnCancel(() -> {
                    if (contains(streamId) && connection.availability() > 0.0) {
                        connection
                            .sendOne(Frame.Cancel.from(streamId))
                            .doOnError(errorConsumer::accept)
                            .subscribe();
                    }
                    subscribedRequest.cancel();
                })
                .doFinally(s ->
                    removeReceiver(streamId)
                );
        });
    }

    private Flux<Payload> handleStreamResponse(Flux<Payload> request, FrameType requestType) {
        return started.thenMany(() -> {
            int streamId = nextStreamId();

            UnicastProcessor<Payload> receiver = UnicastProcessor.create();

            Flux<Frame> requestFrames =
                request
                    .transform(f -> {
                        LimitableRequestPublisher<Payload> wrapped = LimitableRequestPublisher.wrap(f);
                        synchronized (ClientReactiveSocket.this) {
                            senders.put(streamId, wrapped);
                            receivers.put(streamId, receiver);
                        }

                        return wrapped;
                    })
                    .doOnRequest(l -> System.out.println("request n from netty -> "  + l))
                    .map(payload -> Frame.Request.from(streamId, requestType, payload, 1));

            MonoProcessor<Void> subscribedRequests = connection
                .send(requestFrames)
                .doOnError(t -> {
                    errorConsumer.accept(t);
                    receiver.cancel();
                })
                .subscribe();

            return receiver
                .doOnRequest(l -> {
                    if (contains(streamId) && connection.availability() > 0.0) {
                        connection
                            .sendOne(Frame.RequestN.from(streamId, l))
                            .doOnError(receiver::onError)
                            .subscribe();
                    }
                })
                .doOnError(t -> {
                    if (contains(streamId) && connection.availability() > 0.0) {
                        connection
                            .sendOne(Frame.Error.from(streamId, t))
                            .doOnError(errorConsumer::accept)
                            .subscribe();
                    }
                })
                .doOnCancel(() -> {
                    if (contains(streamId) && connection.availability() > 0.0) {
                        connection
                            .sendOne(Frame.Cancel.from(streamId))
                            .doOnError(errorConsumer::accept)
                            .subscribe();
                    }
                    subscribedRequests.cancel();
                })
                .doFinally(s -> {
                    removeReceiver(streamId);
                    removeSender(streamId);
                });
        });
    }

    private boolean contains(int streamId) {
        synchronized (ClientReactiveSocket.this) {
            return receivers.containsKey(streamId);
        }
    }

    protected void cleanup() {
        // TODO: Stop sending requests first
        if (null != keepAliveSendSub) {
            keepAliveSendSub.dispose();
        }
    }

    private void handleIncomingFrames(Frame frame) {
        int streamId = frame.getStreamId();
        FrameType type = frame.getType();
        if (streamId == 0) {
            handleStreamZero(type, frame);
        } else {
            handleFrame(streamId, type, frame);
        }
    }

    private void handleStreamZero(FrameType type, Frame frame) {
        switch (type) {
            case ERROR:
                throw Exceptions.from(frame);
            case LEASE: {
                if (leaseConsumer != null) {
                    leaseConsumer.accept(new LeaseImpl(frame));
                }
                break;
            }
            case KEEPALIVE:
                if (!Frame.Keepalive.hasRespondFlag(frame)) {
                    // Respond flag absent => Ack of KeepAlive
                    keepAliveProvider.ack();
                }
                break;
            default:
                // Ignore unknown frames. Throwing an error will close the socket.
                errorConsumer.accept(new IllegalStateException("Client received supported frame on stream 0: "
                    + frame.toString()));
        }
    }

    @SuppressWarnings("unchecked")
    private void handleFrame(int streamId, FrameType type, Frame frame) {
        Subscriber<? super Frame> receiver;
        synchronized (this) {
            receiver = receivers.get(streamId);
        }
        if (receiver == null) {
            handleMissingResponseProcessor(streamId, type, frame);
        } else {
            switch (type) {
                case ERROR:
                    receiver.onError(Exceptions.from(frame));
                    removeReceiver(streamId);
                    break;
                case NEXT_COMPLETE:
                    receiver.onNext(frame);
                    receiver.onComplete();
                    break;
                case CANCEL: {
                    LimitableRequestPublisher sender;
                    synchronized (this) {
                        sender = senders.remove(streamId);
                        removeReceiver(streamId);
                    }
                    if (sender != null) {
                        sender.cancel();
                    }
                    break;
                }
                case NEXT:
                    receiver.onNext(frame);
                    break;
                case REQUEST_N: {
                    LimitableRequestPublisher sender;
                    synchronized (this) {
                        sender = senders.get(streamId);
                    }
                    if (sender != null) {
                        int n = Frame.RequestN.requestN(frame);
                        sender.increaseRequestLimit(n);
                    }
                    break;
                }
                case COMPLETE:
                    receiver.onComplete();
                    synchronized (this) {
                        receivers.remove(streamId);
                    }
                    break;
                default:
                    throw new IllegalStateException(
                        "Client received supported frame on stream " + streamId + ": " + frame.toString());
            }
        }
    }

    private void handleMissingResponseProcessor(int streamId, FrameType type, Frame frame) {
        if (!streamIdSupplier.isBeforeOrCurrent(streamId)) {
            if (type == FrameType.ERROR) {
                // message for stream that has never existed, we have a problem with
                // the overall connection and must tear down
                String errorMessage = getByteBufferAsString(frame.getData());

                throw new IllegalStateException("Client received error for non-existent stream: "
                    + streamId + " Message: " + errorMessage);
            } else {
                throw new IllegalStateException("Client received message for non-existent stream: " + streamId +
                    ", frame type: " + type);
            }
        }
        // receiving a frame after a given stream has been cancelled/completed,
        // so ignore (cancellation is async so there is a race condition)
    }

    private int nextStreamId() {
        return streamIdSupplier.nextStreamId();
    }

    private static String getByteBufferAsString(ByteBuffer bb) {
        final byte[] bytes = new byte[bb.remaining()];
        bb.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private synchronized void removeReceiver(int streamId) {
        receivers.remove(streamId);
    }

    private synchronized void removeSender(int streamId) {
        senders.remove(streamId);
    }
}
