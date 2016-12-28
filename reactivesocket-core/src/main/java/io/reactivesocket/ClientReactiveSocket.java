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
import io.reactivesocket.internal.RemoteReceiver;
import io.reactivesocket.internal.RemoteSender;
import io.reactivesocket.lease.Lease;
import io.reactivesocket.lease.LeaseImpl;
import io.reactivesocket.reactivestreams.extensions.DefaultSubscriber;
import io.reactivesocket.reactivestreams.extensions.Px;
import io.reactivesocket.reactivestreams.extensions.internal.ValidatingSubscription;
import io.reactivesocket.reactivestreams.extensions.internal.subscribers.CancellableSubscriber;
import io.reactivesocket.reactivestreams.extensions.internal.subscribers.Subscribers;
import org.agrona.collections.Int2ObjectHashMap;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

import static io.reactivesocket.reactivestreams.extensions.internal.subscribers.Subscribers.*;

/**
 * Client Side of a ReactiveSocket socket. Sends {@link Frame}s
 * to a {@link ServerReactiveSocket}
 */
public class ClientReactiveSocket implements ReactiveSocket {

    private final DuplexConnection connection;
    private final Consumer<Throwable> errorConsumer;
    private final StreamIdSupplier streamIdSupplier;
    private final KeepAliveProvider keepAliveProvider;

    private final Int2ObjectHashMap<Subscription> senders;
    private final Int2ObjectHashMap<Subscriber<Frame>> receivers;

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
        return Px.defer(() -> {
            final int streamId = nextStreamId();
            final Frame requestFrame = Frame.Request.from(streamId, FrameType.FIRE_AND_FORGET, payload, 0);
            return connection.sendOne(requestFrame);
        });
    }

    @Override
    public Publisher<Payload> requestResponse(Payload payload) {
        return handleRequestResponse(payload);
    }

    @Override
    public Publisher<Payload> requestStream(Payload payload) {
        return handleStreamResponse(Px.just(payload), FrameType.REQUEST_STREAM);
    }

    @Override
    public Publisher<Payload> requestSubscription(Payload payload) {
        return handleStreamResponse(Px.just(payload), FrameType.REQUEST_SUBSCRIPTION);
    }

    @Override
    public Publisher<Payload> requestChannel(Publisher<Payload> payloads) {
        return handleStreamResponse(Px.from(payloads), FrameType.REQUEST_CHANNEL);
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

    private Publisher<Payload> handleRequestResponse(final Payload payload) {
        return Px.create(subscriber -> {
            int streamId = nextStreamId();
            final Frame requestFrame = Frame.Request.from(streamId, FrameType.REQUEST_RESPONSE, payload, 1);
            synchronized (this) {
                @SuppressWarnings("rawtypes")
                Subscriber raw = subscriber;
                @SuppressWarnings("unchecked")
                Subscriber<Frame> fs = raw;
                receivers.put(streamId, fs);
            }
            Px.concatEmpty(connection.sendOne(requestFrame), Px.never())
              .cast(Payload.class)
              .doOnCancel(() -> {
                  if (connection.availability() > 0.0) {
                      connection.sendOne(Frame.Cancel.from(streamId))
                                .subscribe(DefaultSubscriber.defaultInstance());
                  }
              })
              .subscribe(subscriber);
        });
    }

    private Publisher<Payload> handleStreamResponse(Px<Payload> request, FrameType requestType) {
        return Px.defer(() -> {
            int streamId = nextStreamId();
            RemoteSender sender = new RemoteSender(request.map(payload -> Frame.Request.from(streamId, requestType,
                                                                                             payload, 1)),
                                                   removeSenderLambda(streamId), streamId);
            Publisher<Frame> src = s -> {
                CancellableSubscriber<Void> sendSub = doOnError(throwable -> {
                    s.onError(throwable);
                });
                ValidatingSubscription<? super Frame> sub = ValidatingSubscription.create(s, () -> {
                    sendSub.cancel();
                }, requestN -> {
                    transportReceiveSubscription.request(requestN);
                });
                connection.send(sender).subscribe(sendSub);
                s.onSubscribe(sub);
            };

            RemoteReceiver receiver = new RemoteReceiver(src, connection, streamId, removeReceiverLambda(streamId),
                                                         true);
            registerSenderReceiver(streamId, sender, receiver);
            return receiver;
        });
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
        Subscriber<Frame> receiver;
        synchronized (this) {
            receiver = receivers.get(streamId);
        }
        if (receiver == null) {
            handleMissingResponseProcessor(streamId, type, frame);
        } else {
            switch (type) {
                case ERROR:
                    receiver.onError(Exceptions.from(frame));
                    synchronized (this) {
                        receivers.remove(streamId);
                    }
                    break;
                case NEXT_COMPLETE:
                    receiver.onNext(frame);
                    receiver.onComplete();
                    synchronized (this) {
                        receivers.remove(streamId);
                    }
                    break;
                case CANCEL: {
                    Subscription sender;
                    synchronized (this) {
                        sender = senders.remove(streamId);
                        receivers.remove(streamId);
                    }
                    if (sender != null) {
                        sender.cancel();
                    }
                    receiver.onError(new CancelException("cancelling stream id " + streamId));
                    break;
                }
                case NEXT:
                    receiver.onNext(frame);
                    break;
                case REQUEST_N: {
                    Subscription sender;
                    synchronized (this) {
                        sender = senders.get(streamId);
                    }
                    if (sender != null) {
                        int n = Frame.RequestN.requestN(frame);
                        sender.request(n);
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

    private Runnable removeReceiverLambda(int streamId) {
        return () -> {
            removeReceiver(streamId);
        };
    }

    private synchronized void removeReceiver(int streamId) {
        receivers.remove(streamId);
    }

    private Runnable removeSenderLambda(int streamId) {
        return () -> {
            removeSender(streamId);
        };
    }

    private synchronized void removeSender(int streamId) {
        senders.remove(streamId);
    }

    private synchronized void registerSenderReceiver(int streamId, Subscription sender, Subscriber<Frame> receiver) {
        senders.put(streamId, sender);
        receivers.put(streamId, receiver);
    }
}
