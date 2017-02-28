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
import io.reactivesocket.events.EventListener;
import io.reactivesocket.events.EventListener.RequestType;
import io.reactivesocket.events.EventPublishingSocket;
import io.reactivesocket.events.EventPublishingSocketImpl;
import io.reactivesocket.exceptions.ApplicationException;
import io.reactivesocket.exceptions.SetupException;
import io.reactivesocket.frame.FrameHeaderFlyweight;
import io.reactivesocket.internal.*;
import io.reactivesocket.lease.LeaseEnforcingSocket;
import io.reactivesocket.util.Clock;
import org.agrona.collections.Int2ObjectHashMap;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.function.Consumer;

import static io.reactivesocket.events.EventListener.RequestType.*;

/**
 * Server side ReactiveSocket. Receives {@link Frame}s from a
 * {@link ClientReactiveSocket}
 */
public class ServerReactiveSocket implements ReactiveSocket {

    private final DuplexConnection connection;
    private final Flux<Frame> serverInput;
    private final Consumer<Throwable> errorConsumer;
    private final EventPublisher<? extends EventListener> eventPublisher;

    private final Int2ObjectHashMap<Subscription> subscriptions;
    private final Int2ObjectHashMap<RemoteReceiver> channelProcessors;

    private final ReactiveSocket requestHandler;
    private Subscription receiversSubscription;
    private final EventPublishingSocket eventPublishingSocket;

    public ServerReactiveSocket(DuplexConnection connection, ReactiveSocket requestHandler,
                                boolean clientHonorsLease, Consumer<Throwable> errorConsumer,
                                EventPublisher<? extends EventListener> eventPublisher) {
        this.requestHandler = requestHandler;
        this.connection = connection;
        serverInput = connection.receive();
        this.errorConsumer = new KnownErrorFilter(errorConsumer);
        this.eventPublisher = eventPublisher;
        subscriptions = new Int2ObjectHashMap<>();
        channelProcessors = new Int2ObjectHashMap<>();
        eventPublishingSocket = eventPublisher.isEventPublishingEnabled()?
                new EventPublishingSocketImpl(eventPublisher, false) : EventPublishingSocket.DISABLED;

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
    public Flux<Payload> requestSubscription(Payload payload) {
        return requestHandler.requestSubscription(payload);
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
        return Mono.defer(() -> {
            cleanup();
            return Mono.empty();
        }).thenEmpty(connection.close());
    }

    @Override
    public Mono<Void> onClose() {
        return connection.onClose();
    }

    public ServerReactiveSocket start() {
        serverInput
            .doOnNext(frame -> {
                handleFrame(frame).doOnError(errorConsumer).subscribe();
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

    private Mono<Void> handleFrame(Frame frame) {
        final int streamId = frame.getStreamId();
        try {
            RemoteReceiver receiver;
            switch (frame.getType()) {
                case SETUP:
                    return Mono.error(new IllegalStateException("Setup frame received post setup."));
                case REQUEST_RESPONSE:
                    return handleRequestResponse(streamId, requestResponse(frame));
                case CANCEL:
                    return handleCancelFrame(streamId);
                case KEEPALIVE:
                    return handleKeepAliveFrame(frame);
                case REQUEST_N:
                    return handleRequestN(streamId, frame);
                case REQUEST_STREAM:
                    return doReceive(streamId, requestStream(frame), RequestStream);
                case FIRE_AND_FORGET:
                    return handleFireAndForget(streamId, fireAndForget(frame));
                case REQUEST_SUBSCRIPTION:
                    return doReceive(streamId, requestSubscription(frame), RequestStream);
                case REQUEST_CHANNEL:
                    return handleChannel(streamId, frame);
                case RESPONSE:
                    // TODO: Hook in receiving socket.
                    return Mono.empty();
                case METADATA_PUSH:
                    return metadataPush(frame);
                case LEASE:
                    // Lease must not be received here as this is the server end of the socket which sends leases.
                    return Mono.empty();
                case NEXT:
                    synchronized (channelProcessors) {
                        receiver = channelProcessors.get(streamId);
                    }
                    if (receiver != null) {
                        receiver.onNext(frame);
                    }
                    return Mono.empty();
                case COMPLETE:
                    synchronized (channelProcessors) {
                        receiver = channelProcessors.get(streamId);
                    }
                    if (receiver != null) {
                        receiver.onComplete();
                    }
                    return Mono.empty();
                case ERROR:
                    synchronized (channelProcessors) {
                        receiver = channelProcessors.get(streamId);
                    }
                    if (receiver != null) {
                        receiver.onError(new ApplicationException(frame));
                    }
                    return Mono.empty();
                case NEXT_COMPLETE:
                    synchronized (channelProcessors) {
                        receiver = channelProcessors.get(streamId);
                    }
                    if (receiver != null) {
                        receiver.onNext(frame);
                        receiver.onComplete();
                    }
                    return Mono.empty();
                default:
                    return handleError(streamId, new IllegalStateException("ServerReactiveSocket: Unexpected frame type: "
                        + frame.getType()));
            }
        } catch (Throwable t) {
            Mono<Void> toReturn = handleError(streamId, t);
            // If it's a setup exception re-throw the exception to tear everything down
            if (t instanceof SetupException) {
                toReturn = toReturn.thenEmpty(Mono.error(t));
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
        requestHandler.close().subscribe();
    }

    private Mono<Void> handleRequestResponse(int streamId, Mono<Payload> response) {
        long now = publishSingleFrameReceiveEvents(streamId, RequestResponse);

        Mono<Frame> frames = new MonoOnErrorOrCancelReturn<>(
            response
                .doOnSubscribe(subscription -> {
                    synchronized (this) {
                        subscriptions.put(streamId, subscription);
                    }
                }).map(payload ->
                    Response.from(streamId, FrameType.RESPONSE, payload.getMetadata(), payload.getData(), FrameHeaderFlyweight.FLAGS_RESPONSE_C)
                ).doFinally(signalType -> {
                    synchronized (this) {
                        subscriptions.remove(streamId);
                    }
                }),
            throwable -> Frame.Error.from(streamId, throwable),
            () -> Frame.Cancel.from(streamId)
        );

        return eventPublishingSocket.decorateSend(streamId, connection.send(frames), now, RequestResponse);
    }

    private Mono<Void> doReceive(int streamId, Flux<Payload> response, RequestType requestType) {
        long now = publishSingleFrameReceiveEvents(streamId, requestType);
        Flux<Frame> resp = response
                           .map(payload -> Response.from(streamId, FrameType.RESPONSE, payload));
        RemoteSender sender = new RemoteSender(resp, () -> subscriptions.remove(streamId), streamId, 2);
        subscriptions.put(streamId, sender);
        return eventPublishingSocket.decorateSend(streamId, connection.send(sender), now, requestType);
    }

    private Mono<Void> handleChannel(int streamId, Frame firstFrame) {
        long now = publishSingleFrameReceiveEvents(streamId, RequestChannel);
        int initialRequestN = Request.initialRequestN(firstFrame);
        Frame firstAsNext = Request.from(streamId, FrameType.NEXT, firstFrame, initialRequestN);
        RemoteReceiver receiver = new RemoteReceiver(connection, streamId, () -> removeChannelProcessor(streamId),
            firstAsNext, receiversSubscription, true);
        channelProcessors.put(streamId, receiver);

        Flux<Frame> response = requestChannel(eventPublishingSocket.decorateReceive(streamId, receiver, RequestChannel))
            .map(payload -> Response.from(streamId, FrameType.RESPONSE, payload));

        RemoteSender sender = new RemoteSender(response, () -> removeSubscriptions(streamId), streamId,
            initialRequestN);
        synchronized (this) {
            subscriptions.put(streamId, sender);
        }

        return eventPublishingSocket.decorateSend(streamId, connection.send(sender), now, RequestChannel);
    }

    private Mono<Void> handleFireAndForget(int streamId, Mono<Void> result) {
        return result
            .doOnSubscribe(subscription -> addSubscription(streamId, subscription))
            .doOnError(t -> {
                removeSubscription(streamId);
                errorConsumer.accept(t);
            })
            .doFinally(signalType -> removeSubscription(streamId));
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
            subscription = subscriptions.remove(streamId);
        }

        if (subscription != null) {
            subscription.cancel();
        }

        return Mono.empty();
    }

    private Mono<Void> handleError(int streamId, Throwable t) {
        return connection.sendOne(Frame.Error.from(streamId, t))
            .doOnError(errorConsumer);
    }

    private Mono<Void> handleRequestN(int streamId, Frame frame) {
        Subscription subscription;
        synchronized (this) {
            subscription = subscriptions.get(streamId);
        }
        if (subscription != null) {
            int n = Frame.RequestN.requestN(frame);
            subscription.request(n >= Integer.MAX_VALUE ? Long.MAX_VALUE : n);
        }
        return Mono.empty();
    }

    private synchronized void addSubscription(int streamId, Subscription subscription) {
        subscriptions.put(streamId, subscription);
    }

    private synchronized void removeSubscription(int streamId) {
        subscriptions.remove(streamId);
    }

    private long publishSingleFrameReceiveEvents(int streamId, RequestType requestType) {
        long now = Clock.now();
        if (eventPublisher.isEventPublishingEnabled()) {
            EventListener eventListener = eventPublisher.getEventListener();
            eventListener.requestReceiveStart(streamId, requestType);
            eventListener.requestReceiveComplete(streamId, requestType, Clock.elapsedSince(now), Clock.unit());
        }
        return now;
    }
}
