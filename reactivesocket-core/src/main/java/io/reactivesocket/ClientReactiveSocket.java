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
import io.reactivesocket.exceptions.CancelException;
import io.reactivesocket.exceptions.Exceptions;
import io.reactivesocket.internal.KnownErrorFilter;
import io.reactivesocket.lease.Lease;
import io.reactivesocket.lease.LeaseImpl;
import io.reactivesocket.reactivestreams.extensions.DefaultSubscriber;
import io.reactivesocket.reactivestreams.extensions.Px;
import io.reactivesocket.reactivestreams.extensions.internal.processors.ConnectableUnicastProcessor;
import io.reactivesocket.reactivestreams.extensions.internal.subscribers.CancellableSubscriber;
import io.reactivesocket.reactivestreams.extensions.internal.subscribers.Subscribers;
import org.agrona.collections.Int2ObjectHashMap;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

import static io.reactivesocket.reactivestreams.extensions.internal.subscribers.Subscribers.doOnError;

/**
 * Client Side of a ReactiveSocket socket. Sends {@link Frame}s
 * to a {@link ServerReactiveSocket}
 */
public class ClientReactiveSocket implements ReactiveSocket {

    private final DuplexConnection connection;
    private final Consumer<Throwable> errorConsumer;
    private final StreamIdSupplier streamIdSupplier;
    private final KeepAliveProvider keepAliveProvider;

    private final Int2ObjectHashMap<Processor<Frame, Frame>> senders;
    private final Int2ObjectHashMap<Subscriber<? super Payload>> receivers;

    private volatile Subscription transportReceiveSubscription;
    private CancellableSubscriber<Void> keepAliveSendSub;
    private volatile Consumer<Lease> leaseConsumer; // Provided on start()

    public ClientReactiveSocket(DuplexConnection connection, Consumer<Throwable> errorConsumer,
                                StreamIdSupplier streamIdSupplier, KeepAliveProvider keepAliveProvider) {
        this.connection = connection;
        this.errorConsumer = new KnownErrorFilter(errorConsumer);
        this.streamIdSupplier = streamIdSupplier;
        this.keepAliveProvider = keepAliveProvider;
        senders = new Int2ObjectHashMap<>(256, 0.9f);
        receivers = new Int2ObjectHashMap<>(256, 0.9f);
        connection.onClose().subscribe(Subscribers.cleanup(() -> {
            cleanup();
        }));
    }

    @Override
    public Publisher<Void> fireAndForget(Payload payload) {
        try {
            final int streamId = nextStreamId();
            final Frame requestFrame = Frame.Request.from(streamId, FrameType.FIRE_AND_FORGET, payload, 0);
            return connection.sendOne(requestFrame);
        } catch (Throwable t) {
            return Px.error(t);
        }
    }

    public Publisher<Payload> requestResponse(Payload payload) {
        final int streamId = nextStreamId();
        final Frame requestFrame = Frame.Request.from(streamId, FrameType.REQUEST_RESPONSE, payload, 1);

        return doSendReceive(Px.just(requestFrame), streamId, 1, false);
    }

    @Override
    public Publisher<Payload> requestStream(Payload payload) {
        final int streamId = nextStreamId();
        final Frame requestFrame = Frame.Request.from(streamId, FrameType.REQUEST_STREAM, payload, 1);

        return doSendReceive(Px.just(requestFrame), streamId, 1, true);
    }

    @Override
    public Publisher<Payload> requestSubscription(Payload payload) {
        final int streamId = nextStreamId();
        final Frame requestFrame = Frame.Request.from(streamId, FrameType.REQUEST_SUBSCRIPTION, payload, 1);

        return doSendReceive(Px.just(requestFrame), streamId, 1, true);
    }

    @Override
    public Publisher<Payload> requestChannel(Publisher<Payload> payloads) {
        final int streamId = nextStreamId();
        Px<Frame> frames = Px
            .from(payloads)
            .map(payload -> Frame.Request.from(streamId, FrameType.REQUEST_CHANNEL, payload, 1));
        return doSendReceive(frames, streamId, 1, true);
    }

    private Publisher<Payload> doSendReceive(final Publisher<Frame> payload, final int streamId, final int initialRequestN, final boolean sendRequestN) {
        ConnectableUnicastProcessor<Frame> sender = new ConnectableUnicastProcessor<>();

        synchronized (this) {
            senders.put(streamId, sender);
        }

        final Runnable cleanup = () -> {
            synchronized (this) {
                receivers.remove(streamId);
                senders.remove(streamId);
            }
        };

        return Px
            .<Payload>create(subscriber -> {
                synchronized (this) {
                    receivers.put(streamId, subscriber);
                }

                payload.subscribe(sender);

                subscriber.onSubscribe(new Subscription() {

                    @Override
                    public void request(long n) {
                        if (sendRequestN) {
                            sender.onNext(Frame.RequestN.from(streamId, n));
                        }
                    }

                    @Override
                    public void cancel() {
                        sender.onNext(Frame.Cancel.from(streamId));
                        sender.cancel();
                    }
                });

                try {
                    Px.from(connection.send(sender))
                      .doOnError(th -> subscriber.onError(th))
                      .subscribe(DefaultSubscriber.defaultInstance());
                } catch (Throwable t) {
                    subscriber.onError(t);
                }
            })
            .doOnRequest(subscription -> sender.start(initialRequestN))
            .doOnTerminate(cleanup);
    }

    @Override
    public Publisher<Void> metadataPush(Payload payload) {
        final Frame requestFrame = Frame.Request.from(0, FrameType.METADATA_PUSH, payload, 0);
        return connection.sendOne(requestFrame);
    }

    @Override
    public double availability() {
        return connection.availability();
    }

    @Override
    public Publisher<Void> close() {
        return Px.concatEmpty(Px.defer(() -> {
            cleanup();
            return Px.empty();
        }), connection.close());
    }

    @Override
    public Publisher<Void> onClose() {
        return connection.onClose();
    }

    public ClientReactiveSocket start(Consumer<Lease> leaseConsumer) {
        this.leaseConsumer = leaseConsumer;
        startKeepAlive();
        startReceivingRequests();
        return this;
    }

    private void startKeepAlive() {
        keepAliveSendSub = doOnError(errorConsumer);
        connection.send(Px.from(keepAliveProvider.ticks())
            .map(i -> Frame.Keepalive.from(Frame.NULL_BYTEBUFFER, true)))
            .subscribe(keepAliveSendSub);
    }

    private void startReceivingRequests() {
        Px
            .from(connection.receive())
            .doOnSubscribe(subscription -> transportReceiveSubscription = subscription)
            .doOnNext(this::handleIncomingFrames)
            .subscribe();
    }

    protected void cleanup() {
        // TODO: Stop sending requests first
        if (null != keepAliveSendSub) {
            keepAliveSendSub.cancel();
        }
        if (null != transportReceiveSubscription) {
            transportReceiveSubscription.cancel();
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
        Subscriber<? super Payload> receiver;
        synchronized (this) {
            receiver = receivers.get(streamId);
        }
        if (receiver == null) {
            handleMissingResponseProcessor(streamId, type, frame);
        } else {
            switch (type) {
                case ERROR:
                    receiver.onError(Exceptions.from(frame));
                    break;
                case NEXT_COMPLETE:
                    receiver.onNext(frame);
                    receiver.onComplete();
                    break;
                case CANCEL: {
                    Processor sender;
                    synchronized (ClientReactiveSocket.this) {
                        sender = senders.remove(streamId);
                        receivers.remove(streamId);
                    }
                    if (sender != null) {
                        ((ConnectableUnicastProcessor) sender).cancel();
                    }
                    receiver.onError(new CancelException("cancelling stream id " + streamId));
                    break;
                }
                case NEXT:
                    receiver.onNext(frame);
                    break;
                case REQUEST_N: {
                    Processor sender;
                    synchronized (ClientReactiveSocket.this) {
                        sender = senders.get(streamId);
                    }
                    if (sender != null) {
                        int n = Frame.RequestN.requestN(frame);
                        ((ConnectableUnicastProcessor) sender).requestMore(n);
                    }
                    break;
                }
                case COMPLETE:
                    receiver.onComplete();
                    break;
                default:
                    throw new IllegalStateException(
                        "Client received supported frame on stream " + streamId + ": " + frame.toString());
            }
        }
    }

    private void handleMissingResponseProcessor(int streamId, FrameType type, Frame frame) {
        if (!streamIdSupplier.isValid(streamId)) {
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


}
