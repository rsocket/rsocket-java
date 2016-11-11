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
import io.reactivesocket.Frame.Response;
import io.reactivesocket.exceptions.ApplicationException;
import io.reactivesocket.exceptions.RejectedSetupException;
import io.reactivesocket.exceptions.SetupException;
import io.reactivesocket.frame.FrameHeaderFlyweight;
import io.reactivesocket.frame.SetupFrameFlyweight;
import io.reactivesocket.internal.KnownErrorFilter;
import io.reactivesocket.internal.RemoteReceiver;
import io.reactivesocket.internal.RemoteSender;
import io.reactivesocket.lease.LeaseEnforcingSocket;
import io.reactivesocket.reactivestreams.extensions.Px;
import io.reactivesocket.reactivestreams.extensions.internal.subscribers.Subscribers;
import org.agrona.collections.Int2ObjectHashMap;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import java.util.Collection;
import java.util.function.Consumer;

/**
 * Server side ReactiveSocket. Receives {@link Frame}s from a
 * {@link ClientReactiveSocket}
 */
public class ServerReactiveSocket implements ReactiveSocket {

    private final DuplexConnection connection;
    private final Publisher<Frame> serverInput;
    private final Consumer<Throwable> errorConsumer;

    private final Int2ObjectHashMap<Subscription> subscriptions;
    private final Int2ObjectHashMap<RemoteReceiver> channelProcessors;

    private final ReactiveSocket requestHandler;
    private Subscription receiversSubscription;
    public ServerReactiveSocket(DuplexConnection connection, ReactiveSocket requestHandler,
                                boolean clientHonorsLease, Consumer<Throwable> errorConsumer) {
        this.requestHandler = requestHandler;
        this.connection = connection;
        serverInput = connection.receive();
        this.errorConsumer = new KnownErrorFilter(errorConsumer);
        subscriptions = new Int2ObjectHashMap<>();
        channelProcessors = new Int2ObjectHashMap<>();
        Px.from(connection.onClose()).subscribe(Subscribers.cleanup(() -> {
            cleanup();
        }));
        if (requestHandler instanceof LeaseEnforcingSocket) {
            LeaseEnforcingSocket enforcer = (LeaseEnforcingSocket) requestHandler;
            enforcer.acceptLeaseSender(lease -> {
                if (!clientHonorsLease) {
                    return;
                }
                Frame leaseFrame = Lease.from(lease.getTtl(), lease.getAllowedRequests(), lease.metadata());
                Px.from(connection.sendOne(leaseFrame))
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
    public Publisher<Void> fireAndForget(Payload payload) {
        return requestHandler.fireAndForget(payload);
    }

    @Override
    public Publisher<Payload> requestResponse(Payload payload) {
        return requestHandler.requestResponse(payload);
    }

    @Override
    public Publisher<Payload> requestStream(Payload payload) {
        return requestHandler.requestStream(payload);
    }

    @Override
    public Publisher<Payload> requestSubscription(Payload payload) {
        return requestHandler.requestSubscription(payload);
    }

    @Override
    public Publisher<Payload> requestChannel(Publisher<Payload> payloads) {
        return requestHandler.requestChannel(payloads);
    }

    @Override
    public Publisher<Void> metadataPush(Payload payload) {
        return requestHandler.metadataPush(payload);
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

    public ServerReactiveSocket start() {
        Px.from(serverInput)
            .doOnNext(frame -> {
                handleFrame(frame).subscribe(Subscribers.doOnError(errorConsumer));
            })
            .doOnError(t -> {
                errorConsumer.accept(t);

                //TODO: This should be error?

                Collection<Subscription> values;
                synchronized (this) {
                    values = subscriptions.values();
                }
                values
                    .forEach(Subscription::cancel);
            })
            .doOnSubscribe(subscription -> {
                receiversSubscription = new Subscription() {
                    @Override
                    public void request(long n) {
                        subscription.request(n);
                    }

                    @Override
                    public void cancel() {
                        subscription.cancel();
                    }
                };
            })
            .subscribe();
        return this;
    }

    private Publisher<Void> handleFrame(Frame frame) {
        final int streamId = frame.getStreamId();
        try {
            RemoteReceiver receiver;
            switch (frame.getType()) {
                case SETUP:
                    return Px.error(new IllegalStateException("Setup frame received post setup."));
                case REQUEST_RESPONSE:
                    return handleReceive(streamId, requestResponse(frame));
                case CANCEL:
                    return handleCancelFrame(streamId);
                case KEEPALIVE:
                    return handleKeepAliveFrame(frame);
                case REQUEST_N:
                    return handleRequestN(streamId, frame);
                case REQUEST_STREAM:
                    return doReceive(streamId, requestStream(frame));
                case FIRE_AND_FORGET:
                    return handleFireAndForget(streamId, fireAndForget(frame));
                case REQUEST_SUBSCRIPTION:
                    return doReceive(streamId, requestSubscription(frame));
                case REQUEST_CHANNEL:
                    return handleChannel(streamId, frame);
                case RESPONSE:
                    // TODO: Hook in receiving socket.
                    return Px.empty();
                case METADATA_PUSH:
                    return metadataPush(frame);
                case LEASE:
                    // Lease must not be received here as this is the server end of the socket which sends leases.
                    return Px.empty();
                case NEXT:
                    synchronized (channelProcessors) {
                        receiver = channelProcessors.get(streamId);
                    }
                    if (receiver != null) {
                        receiver.onNext(frame);
                    }
                    return Px.empty();
                case COMPLETE:
                    synchronized (channelProcessors) {
                        receiver = channelProcessors.get(streamId);
                    }
                    if (receiver != null) {
                        receiver.onComplete();
                    }
                    return Px.empty();
                case ERROR:
                    synchronized (channelProcessors) {
                        receiver = channelProcessors.get(streamId);
                    }
                    if (receiver != null) {
                        receiver.onError(new ApplicationException(frame));
                    }
                    return Px.empty();
                case NEXT_COMPLETE:
                    synchronized (channelProcessors) {
                        receiver = channelProcessors.get(streamId);
                    }
                    if (receiver != null) {
                        receiver.onNext(frame);
                        receiver.onComplete();
                    }
                    return Px.empty();
                default:
                    return handleError(streamId, new IllegalStateException("ServerReactiveSocket: Unexpected frame type: "
                        + frame.getType()));
            }
        } catch (Throwable t) {
            Publisher<Void> toReturn = handleError(streamId, t);
            // If it's a setup exception re-throw the exception to tear everything down
            if (t instanceof SetupException) {
                toReturn = Px.concatEmpty(toReturn, Px.error(t));
            }
            return toReturn;
        }
    }

    private synchronized void removeChannelProcessor(int streamId) {
        channelProcessors.remove(streamId);
    }

    private synchronized void removeSubscriptions(int streamId) {
        subscriptions.remove(streamId);
    }

    private synchronized void cleanup() {
        subscriptions.values().forEach(Subscription::cancel);
        subscriptions.clear();
        channelProcessors.values().forEach(RemoteReceiver::cancel);
        subscriptions.clear();
        requestHandler.close().subscribe(Subscribers.empty());
    }

    private Publisher<Void> handleReceive(int streamId, Publisher<Payload> response) {
        final Runnable cleanup = () -> {
            synchronized (this) {
                subscriptions.remove(streamId);
            }

        };

        Px<Frame> frames =
            Px
                .from(response)
                .doOnSubscribe(subscription -> {
                    synchronized (this) {
                        subscriptions.put(streamId, subscription);
                    }
                })
                .map(payload -> Response
                    .from(streamId, FrameType.RESPONSE, payload.getMetadata(), payload.getData(), FrameHeaderFlyweight.FLAGS_RESPONSE_C))
                .doOnComplete(cleanup)
                .emitOnCancelOrError(
                    // on cancel
                    () -> {
                        cleanup.run();
                        return Frame.Cancel.from(streamId);
                    },
                    // on error
                    throwable -> {
                        cleanup.run();
                        return Frame.Error.from(streamId, throwable);
                    });

        return Px.from(connection.send(frames));

    }

    private Publisher<Void> doReceive(int streamId, Publisher<Payload> response) {
        Px<Frame> resp = Px.from(response)
                           .map(payload -> Response.from(streamId, FrameType.RESPONSE, payload));
        RemoteSender sender = new RemoteSender(resp, () -> subscriptions.remove(streamId), streamId, 2);
        subscriptions.put(streamId, sender);
        return connection.send(sender);
    }

    private Publisher<Void> handleChannel(int streamId, Frame firstFrame) {
        int initialRequestN = Request.initialRequestN(firstFrame);
        Frame firstAsNext = Request.from(streamId, FrameType.NEXT, firstFrame, initialRequestN);
        RemoteReceiver receiver = new RemoteReceiver(connection, streamId, () -> removeChannelProcessor(streamId),
            firstAsNext, receiversSubscription, true);
        channelProcessors.put(streamId, receiver);

        Px<Frame> response = Px.from(requestChannel(receiver))
            .map(payload -> Response.from(streamId, FrameType.RESPONSE, payload));

        RemoteSender sender = new RemoteSender(response, () -> removeSubscriptions(streamId), streamId,
            initialRequestN);
        synchronized (this) {
            subscriptions.put(streamId, sender);
        }

        return connection.send(sender);
    }

    private Publisher<Void> handleFireAndForget(int streamId, Publisher<Void> result) {
        return Px.from(result)
            .doOnSubscribe(subscription -> addSubscription(streamId, subscription))
            .doOnError(t -> {
                removeSubscription(streamId);
                errorConsumer.accept(t);
            })
            .doOnComplete(() -> removeSubscription(streamId));
    }

    private Publisher<Void> handleKeepAliveFrame(Frame frame) {
        if (Frame.Keepalive.hasRespondFlag(frame)) {
            return Px.from(connection.sendOne(Frame.Keepalive.from(Frame.NULL_BYTEBUFFER, false)))
                .doOnError(errorConsumer);
        }
        return Px.empty();
    }

    private Publisher<Void> handleCancelFrame(int streamId) {
        Subscription subscription;
        synchronized (this) {
            subscription = subscriptions.remove(streamId);
        }

        if (subscription != null) {
            subscription.cancel();
        }

        return Px.empty();
    }

    private Publisher<Void> handleError(int streamId, Throwable t) {
        return Px.from(connection.sendOne(Frame.Error.from(streamId, t)))
            .doOnError(errorConsumer);
    }

    private Px<Void> handleRequestN(int streamId, Frame frame) {
        Subscription subscription;
        synchronized (this) {
            subscription = subscriptions.get(streamId);
        }
        if (subscription != null) {
            int n = Frame.RequestN.requestN(frame);
            subscription.request(n >= Integer.MAX_VALUE ? Long.MAX_VALUE : n);
        }
        return Px.empty();
    }

    private synchronized void addSubscription(int streamId, Subscription subscription) {
        subscriptions.put(streamId, subscription);
    }

    private synchronized void removeSubscription(int streamId) {
        subscriptions.remove(streamId);
    }
}
