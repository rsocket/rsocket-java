/*
 * Copyright 2016 Netflix, Inc.
 * <p>
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *  <p>
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  <p>
 *  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations under the License.
 */

package io.reactivesocket.internal;

import io.reactivesocket.ConnectionSetupHandler;
import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.FrameType;
import io.reactivesocket.LeaseGovernor;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.RequestHandler;
import io.reactivesocket.exceptions.InvalidSetupException;
import io.reactivesocket.exceptions.RejectedException;
import io.reactivesocket.exceptions.SetupException;
import io.reactivesocket.internal.frame.FrameHeaderFlyweight;
import io.reactivesocket.internal.frame.SetupFrameFlyweight;
import io.reactivesocket.internal.rx.EmptyDisposable;
import io.reactivesocket.internal.rx.EmptySubscription;
import io.reactivesocket.rx.Completable;
import io.reactivesocket.rx.Disposable;
import io.reactivesocket.rx.Observer;
import org.agrona.collections.Int2ObjectHashMap;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * Protocol implementation abstracted over a {@link DuplexConnection}.
 * <p>
 * Concrete implementations of {@link DuplexConnection} over TCP, WebSockets,
 * Aeron, etc can be passed to this class for protocol handling. The request
 * handlers passed in at creation will be invoked
 * for each request over the connection.
 */
public class Responder {
    private final static Disposable CANCELLED = EmptyDisposable.INSTANCE;

    private final DuplexConnection connection;
    private final ConnectionSetupHandler connectionHandler; // for server
    private final RequestHandler clientRequestHandler; // for client
    private final Consumer<Throwable> errorStream;
    private volatile LeaseGovernor leaseGovernor;
    private long timeOfLastKeepalive;
    private final Consumer<ConnectionSetupPayload> setupCallback;
    private final boolean isServer;
    private final AtomicReference<Disposable> transportSubscription = new AtomicReference<>();

    private Responder(
            boolean isServer,
            DuplexConnection connection,
            ConnectionSetupHandler connectionHandler,
            RequestHandler requestHandler,
            LeaseGovernor leaseGovernor,
            Consumer<Throwable> errorStream,
            Consumer<ConnectionSetupPayload> setupCallback
    ) {
        this.isServer = isServer;
        this.connection = connection;
        this.connectionHandler = connectionHandler;
        this.clientRequestHandler = requestHandler;
        this.leaseGovernor = leaseGovernor;
        this.errorStream = errorStream;
        this.timeOfLastKeepalive = System.nanoTime();
        this.setupCallback = setupCallback;
    }

    /**
     * @param connectionHandler Handle connection setup and set up request
     *                          handling.
     * @param errorStream A {@link Consumer<Throwable>} which will receive
     *                    all errors that occurs processing requests.
     *                    This include fireAndForget which ONLY emit errors
     *                    server-side via this mechanism.
     * @return responder instance
     */
    public static Responder createServerResponder(
            DuplexConnection connection,
            ConnectionSetupHandler connectionHandler,
            LeaseGovernor leaseGovernor,
            Consumer<Throwable> errorStream,
            Completable responderCompletable,
            Consumer<ConnectionSetupPayload> setupCallback,
            ReactiveSocket reactiveSocket
    ) {
        Responder responder = new Responder(true, connection, connectionHandler, null,
                leaseGovernor, errorStream, setupCallback);
        responder.start(responderCompletable, reactiveSocket);
        return responder;
    }

    public static Responder createServerResponder(
            DuplexConnection connection,
            ConnectionSetupHandler connectionHandler,
            LeaseGovernor leaseGovernor,
            Consumer<Throwable> errorStream,
            Completable responderCompletable,
            ReactiveSocket reactiveSocket
    ) {
        return createServerResponder(connection, connectionHandler, leaseGovernor,
                errorStream, responderCompletable, s -> {}, reactiveSocket);
    }

    public static Responder createClientResponder(
            DuplexConnection connection,
            RequestHandler requestHandler,
            LeaseGovernor leaseGovernor,
            Consumer<Throwable> errorStream,
            Completable responderCompletable,
            ReactiveSocket reactiveSocket
    ) {
        Responder responder = new Responder(false, connection, null, requestHandler,
                leaseGovernor, errorStream, s -> {});
        responder.start(responderCompletable, reactiveSocket);
        return responder;
    }

    /**
     * Send a LEASE frame immediately. Only way a LEASE is sent. Handled
     * entirely by application logic.
     *
     * @param ttl of lease
     * @param numberOfRequests of lease
     */
    public void sendLease(final int ttl, final int numberOfRequests) {
        Frame leaseFrame = Frame.Lease.from(ttl, numberOfRequests, Frame.NULL_BYTEBUFFER);
        connection.addOutput(Publishers.just(leaseFrame), new Completable() {
            @Override
            public void success() {}

            @Override
            public void error(Throwable e) {
                errorStream.accept(new RuntimeException(name() + ": could not send lease ", e));
            }
        });
    }

    /**
     * Return time of last keepalive from client
     *
     * @return time from {@link System#nanoTime()} of last keepalive
     */
    public long timeOfLastKeepalive() {
        return timeOfLastKeepalive;
    }

    private void start(final Completable responderCompletable, ReactiveSocket reactiveSocket) {
        /* state of cancellation subjects during connection */
        final Int2ObjectHashMap<Subscription> cancellationSubscriptions = new Int2ObjectHashMap<>();
        /* streams in flight that can receive REQUEST_N messages */
        final Int2ObjectHashMap<SubscriptionArbiter> inFlight = new Int2ObjectHashMap<>();
        /* bidirectional channels */
        // TODO: should/can we make this optional so that it only gets allocated per connection if
        //       channels are used?
        final Int2ObjectHashMap<UnicastSubject<Payload>> channels = new Int2ObjectHashMap<>();

        final AtomicBoolean childTerminated = new AtomicBoolean(false);

        // subscribe to transport to get Frames
        connection.getInput().subscribe(new Observer<Frame>() {

            @Override
            public void onSubscribe(Disposable d) {
                if (transportSubscription.compareAndSet(null, d)) {
                    // mark that we have completed setup
                    responderCompletable.success();
                } else {
                    // means we already were cancelled
                    d.dispose();
                }
            }

            // null until after first Setup frame
            volatile RequestHandler requestHandler = !isServer ? clientRequestHandler : null;

            @Override
            public void onNext(Frame requestFrame) {
                final int streamId = requestFrame.getStreamId();
                if (requestHandler == null) { // this will only happen when isServer==true
                    if (childTerminated.get()) {
                        // already terminated, but still receiving latent messages...
                        // ignore them while shutdown occurs
                        return;
                    }
                    if (requestFrame.getType() == FrameType.SETUP) {
                        final ConnectionSetupPayload connectionSetupPayload =
                            ConnectionSetupPayload.create(requestFrame);
                        try {
                            int version = Frame.Setup.version(requestFrame);
                            if (version != SetupFrameFlyweight.CURRENT_VERSION) {
                                throw new SetupException(name() + ": unsupported protocol version: " + version);
                            }

                            // accept setup for ReactiveSocket/Requester usage
                            setupCallback.accept(connectionSetupPayload);
                            // handle setup
                            requestHandler = connectionHandler.apply(connectionSetupPayload, reactiveSocket);
                        } catch (SetupException setupException) {
                            setupErrorAndTearDown(connection, setupException);
                        } catch (Throwable e) {
                            InvalidSetupException exc = new InvalidSetupException(e.getMessage());
                            setupErrorAndTearDown(connection, exc);
                        }

                        // the L bit set must wait until the application logic explicitly sends
                        // a LEASE. ConnectionSetupPlayload knows of bits being set.
                        if (connectionSetupPayload.willClientHonorLease()) {
                            leaseGovernor.register(Responder.this);
                        } else {
                            leaseGovernor = LeaseGovernor.UNLIMITED_LEASE_GOVERNOR;
                        }

                        // TODO: handle keepalive logic here
                    } else {
                        setupErrorAndTearDown(connection,
                            new InvalidSetupException(name() + ": Setup frame missing"));
                    }
                } else {
                    Publisher<Frame> responsePublisher = null;
                    if (leaseGovernor.accept(Responder.this, requestFrame)) {
                    try {
                        if (requestFrame.getType() == FrameType.REQUEST_RESPONSE) {
                            responsePublisher = handleRequestResponse(
                                requestFrame, requestHandler, cancellationSubscriptions);
                        } else if (requestFrame.getType() == FrameType.REQUEST_STREAM) {
                            responsePublisher = handleRequestStream(
                                requestFrame, requestHandler, cancellationSubscriptions, inFlight);
                        } else if (requestFrame.getType() == FrameType.FIRE_AND_FORGET) {
                            responsePublisher = handleFireAndForget(
                                requestFrame, requestHandler);
                        } else if (requestFrame.getType() == FrameType.REQUEST_SUBSCRIPTION) {
                            responsePublisher = handleRequestSubscription(
                                requestFrame, requestHandler, cancellationSubscriptions, inFlight);
                        } else if (requestFrame.getType() == FrameType.REQUEST_CHANNEL) {
                            responsePublisher = handleRequestChannel(
                                requestFrame, requestHandler, channels,
                                cancellationSubscriptions, inFlight);
                        } else if (requestFrame.getType() == FrameType.METADATA_PUSH) {
                            responsePublisher = handleMetadataPush(
                                requestFrame, requestHandler);
                        } else if (requestFrame.getType() == FrameType.CANCEL) {
                            Subscription s;
                            synchronized (Responder.this) {
                                s = cancellationSubscriptions.get(streamId);
                            }
                            if (s != null) {
                                s.cancel();
                            }
                            return;
                        } else if (requestFrame.getType() == FrameType.REQUEST_N) {
                            SubscriptionArbiter inFlightSubscription;
                            synchronized (Responder.this) {
                                inFlightSubscription = inFlight.get(streamId);
                            }
                            if (inFlightSubscription != null) {
                                long requestN = Frame.RequestN.requestN(requestFrame);
                                inFlightSubscription.addApplicationRequest(requestN);
                                return;
                            }
                            // TODO should we do anything if we don't find the stream?
                            // emitting an error is risky as the responder could have
                            // terminated and cleaned up already
                        } else if (requestFrame.getType() == FrameType.KEEPALIVE) {
                            // this client is alive.
                            timeOfLastKeepalive = System.nanoTime();
                            // echo back if flag set
                            if (Frame.Keepalive.hasRespondFlag(requestFrame)) {
                                Frame keepAliveFrame = Frame.Keepalive.from(
                                    requestFrame.getData(), false);
                                responsePublisher = Publishers.just(keepAliveFrame);
                            } else {
                                return;
                            }
                        } else if (requestFrame.getType() == FrameType.LEASE) {
                            // LEASE only concerns the Requester
                        } else {
                            IllegalStateException exc = new IllegalStateException(
                                name() + ": Unexpected prefix: " + requestFrame.getType());
                            responsePublisher = PublisherUtils.errorFrame(streamId, exc);
                        }
                    } catch (Throwable e) {
                        // synchronous try/catch since we execute user functions
                        // in the handlers and they could throw
                        errorStream.accept(
                            new RuntimeException(name() + ": Error in request handling.", e));
                        // error message to user
                        responsePublisher = PublisherUtils.errorFrame(
                                streamId, new RuntimeException(
                                name() + ": Unhandled error processing request"));
                    }
                    } else {
                        RejectedException exception = new RejectedException(name() + ": No associated lease");
                        responsePublisher = PublisherUtils.errorFrame(streamId, exception);
                    }

                    if (responsePublisher != null) {
                        connection.addOutput(responsePublisher, new Completable() {
                            @Override
                            public void success() {
                                // TODO Auto-generated method stub
                            }

                            @Override
                            public void error(Throwable e) {
                                // TODO validate with unit tests
                                if (childTerminated.compareAndSet(false, true)) {
                                    // TODO should we have typed RuntimeExceptions?
                                    errorStream.accept(new RuntimeException("Error writing", e));
                                    cancel();
                                }
                            }
                        });
                    }
                }
            }

            private void setupErrorAndTearDown(
                    DuplexConnection connection,
                    SetupException setupException
            ) {
                // pass the ErrorFrame output, subscribe to write it, await
                // onComplete and then tear down
                final Frame frame = Frame.Error.from(0, setupException);
                connection.addOutput(Publishers.just(frame),
                    new Completable() {
                        @Override
                        public void success() {
                            tearDownWithError(setupException);
                        }
                        @Override
                        public void error(Throwable e) {
                            RuntimeException exc = new RuntimeException(
                                name() + ": Failure outputting SetupException", e);
                            tearDownWithError(exc);
                        }
                    });
            }

            private void tearDownWithError(Throwable se) {
                // TODO unit test that this actually shuts things down
                onError(new RuntimeException(name() + ": Connection Setup Failure", se));
            }

            @Override
            public void onError(Throwable t) {
                // TODO validate with unit tests
                if (childTerminated.compareAndSet(false, true)) {
                    errorStream.accept(t);
                    cancel();
                }
            }

            @Override
            public void onComplete() {
                //TODO validate what is happening here
                // this would mean the connection gracefully shut down, which is unexpected
                if (childTerminated.compareAndSet(false, true)) {
                    cancel();
                }
            }

            private void cancel() {
                // child has cancelled (shutdown the connection or server)
                // TODO validate with unit tests
                Disposable disposable = transportSubscription.getAndSet(CANCELLED);
                if (disposable != null) {
                    // cancel the one that was there if we failed to set the sentinel
                    transportSubscription.get().dispose();
                }
            }

        });
    }

    public void shutdown() {
        Disposable disposable = transportSubscription.getAndSet(CANCELLED);
        if (disposable != null && disposable != CANCELLED) {
            disposable.dispose();
        }
    }

    private Publisher<Frame> handleRequestResponse(
            Frame requestFrame,
            final RequestHandler requestHandler,
            final Int2ObjectHashMap<Subscription> cancellationSubscriptions) {

        final int streamId = requestFrame.getStreamId();
        return child -> {
            Subscription s = new Subscription() {

                final AtomicBoolean started = new AtomicBoolean(false);
                final AtomicReference<Subscription> parent = new AtomicReference<>();

                @Override
                public void request(long n) {
                    if (n > 0 && started.compareAndSet(false, true)) {
                        try {
                            Publisher<Payload> responsePublisher =
                                    requestHandler.handleRequestResponse(requestFrame);
                            responsePublisher.subscribe(new Subscriber<Payload>() {

                                // event emission is serialized so this doesn't need to be atomic
                                int count;

                                @Override
                                public void onSubscribe(Subscription s) {
                                    if (parent.compareAndSet(null, s)) {
                                        // only expect 1 value so we don't need REQUEST_N
                                        s.request(Long.MAX_VALUE);
                                    } else {
                                        s.cancel();
                                        cleanup();
                                    }
                                }

                                @Override
                                public void onNext(Payload v) {
                                    if (++count > 1) {
                                        IllegalStateException exc = new IllegalStateException(
                                            name() + ": RequestResponse expects a single onNext");
                                        onError(exc);
                                    } else {
                                        Frame nextCompleteFrame = Frame.Response.from(
                                                streamId, FrameType.RESPONSE, v.getMetadata(), v.getData(), FrameHeaderFlyweight.FLAGS_RESPONSE_C);
                                        child.onNext(nextCompleteFrame);
                                    }
                                }

                                @Override
                                public void onError(Throwable t) {
                                    child.onNext(Frame.Error.from(streamId, t));
                                    child.onComplete();
                                    cleanup();
                                }

                                @Override
                                public void onComplete() {
                                    if (count != 1) {
                                        IllegalStateException exc = new IllegalStateException(
                                            name() + ": RequestResponse expects a single onNext");
                                        onError(exc);
                                    } else {
                                        child.onComplete();
                                        cleanup();
                                    }
                                }
                            });
                        } catch (Throwable t) {
                            child.onNext(Frame.Error.from(streamId, t));
                            child.onComplete();
                            cleanup();
                        }
                    }
                }

                @Override
                public void cancel() {
                    if (!parent.compareAndSet(null, EmptySubscription.INSTANCE)) {
                        parent.get().cancel();
                        cleanup();
                    }
                }

                private void cleanup() {
                    synchronized(Responder.this) {
                        cancellationSubscriptions.remove(streamId);
                    }
                }

            };
            synchronized(this) {
                cancellationSubscriptions.put(streamId, s);
            }
            child.onSubscribe(s);
        };
    }

    private static final BiFunction<RequestHandler, Payload, Publisher<Payload>>
            requestSubscriptionHandler = RequestHandler::handleSubscription;
    private static final BiFunction<RequestHandler, Payload, Publisher<Payload>>
            requestStreamHandler = RequestHandler::handleRequestStream;

    private Publisher<Frame> handleRequestStream(
            Frame requestFrame,
            final RequestHandler requestHandler,
            final Int2ObjectHashMap<Subscription> cancellationSubscriptions,
            final Int2ObjectHashMap<SubscriptionArbiter> inFlight) {
        return _handleRequestStream(
                requestStreamHandler,
                requestFrame,
                requestHandler,
                cancellationSubscriptions,
                inFlight,
                true
        );
    }

    private Publisher<Frame> handleRequestSubscription(
            Frame requestFrame,
            final RequestHandler requestHandler,
            final Int2ObjectHashMap<Subscription> cancellationSubscriptions,
            final Int2ObjectHashMap<SubscriptionArbiter> inFlight) {
        return _handleRequestStream(
                requestSubscriptionHandler,
                requestFrame,
                requestHandler,
                cancellationSubscriptions,
                inFlight,
                false
        );
    }

    /**
     * Common logic for requestStream and requestSubscription
     *
     * @param handler
     * @param requestFrame
     * @param cancellationSubscriptions
     * @param inFlight
     * @param allowCompletion
     * @return
     */
    private Publisher<Frame> _handleRequestStream(
            BiFunction<RequestHandler, Payload, Publisher<Payload>> handler,
            Frame requestFrame,
            final RequestHandler requestHandler,
            final Int2ObjectHashMap<Subscription> cancellationSubscriptions,
            final Int2ObjectHashMap<SubscriptionArbiter> inFlight,
            final boolean allowCompletion) {
        final int streamId = requestFrame.getStreamId();
        return child -> {
            Subscription s = new Subscription() {

                final AtomicBoolean started = new AtomicBoolean(false);
                final AtomicReference<Subscription> parent = new AtomicReference<>();
                final SubscriptionArbiter arbiter = new SubscriptionArbiter();

                @Override
                public void request(long n) {
                    if(n <= 0) {
                        return;
                    }
                    if (started.compareAndSet(false,  true)) {
                        arbiter.addTransportRequest(n);

                        try {
                            Publisher<Payload> responses =
                                    handler.apply(requestHandler, requestFrame);
                            responses.subscribe(new Subscriber<Payload>() {

                                @Override
                                public void onSubscribe(Subscription s) {
                                    if (parent.compareAndSet(null, s)) {
                                        inFlight.put(streamId, arbiter);
                                        long n = Frame.Request.initialRequestN(requestFrame);
                                        arbiter.addApplicationRequest(n);
                                        arbiter.addApplicationProducer(s);
                                    } else {
                                        s.cancel();
                                        cleanup();
                                    }
                                }

                                @Override
                                public void onNext(Payload v) {
                                    try {
                                        Frame nextFrame = Frame.Response.from(
                                                streamId, FrameType.NEXT, v);
                                        child.onNext(nextFrame);
                                    } catch (Throwable e) {
                                        onError(e);
                                    }
                                }

                                @Override
                                public void onError(Throwable t) {
                                    child.onNext(Frame.Error.from(streamId, t));
                                    child.onComplete();
                                    cleanup();
                                }

                                @Override
                                public void onComplete() {
                                    if (allowCompletion) {
                                        Frame completeFrame = Frame.Response.from(
                                                streamId, FrameType.COMPLETE);
                                        child.onNext(completeFrame);
                                        child.onComplete();
                                        cleanup();
                                    } else {
                                        IllegalStateException exc = new IllegalStateException(
                                            name() + ": Unexpected onComplete occurred on " +
                                                "'requestSubscription'");
                                        onError(exc);
                                    }
                                }
                            });
                        } catch (Throwable t) {
                            child.onNext(Frame.Error.from(streamId, t));
                            child.onComplete();
                            cleanup();
                        }
                    } else {
                        arbiter.addTransportRequest(n);
                    }
                }

                @Override
                public void cancel() {
                    if (!parent.compareAndSet(null, EmptySubscription.INSTANCE)) {
                        parent.get().cancel();
                        cleanup();
                    }
                }

                private void cleanup() {
                    synchronized(Responder.this) {
                        inFlight.remove(streamId);
                        cancellationSubscriptions.remove(streamId);
                    }
                }

            };
            synchronized(this) {
                cancellationSubscriptions.put(streamId, s);
            }
            child.onSubscribe(s);

        };

    }

    private Publisher<Frame> handleFireAndForget(
        Frame requestFrame,
        final RequestHandler requestHandler
    ) {
        try {
            requestHandler.handleFireAndForget(requestFrame).subscribe(completionSubscriber);
        } catch (Throwable e) {
            // we catch these errors here as we don't want anything propagating
            // back to the user on fireAndForget
            errorStream.accept(new RuntimeException(name() + ": Error processing 'fireAndForget'", e));
        }
        // we always treat this as if it immediately completes as we don't want
        // errors passing back to the user
        return Publishers.empty();
    }

    private Publisher<Frame> handleMetadataPush(
        Frame requestFrame,
        final RequestHandler requestHandler
    ) {
        try {
            requestHandler.handleMetadataPush(requestFrame).subscribe(completionSubscriber);
        } catch (Throwable e) {
            // we catch these errors here as we don't want anything propagating
            // back to the user on metadataPush
            errorStream.accept(new RuntimeException(name() + ": Error processing 'metadataPush'", e));
        }
        // we always treat this as if it immediately completes as we don't want
        // errors passing back to the user
        return Publishers.empty();
    }

    /**
     * Reusable for each fireAndForget and metadataPush since no state is shared
     * across invocations. It just passes through errors.
     */
    private final Subscriber<Void> completionSubscriber = new Subscriber<Void>(){
        @Override
        public void onSubscribe(Subscription s) {
            s.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(Void t) {}

        @Override public void onError(Throwable t) {
            errorStream.accept(t);
        }

        @Override public void onComplete() {}
    };

    private Publisher<Frame> handleRequestChannel(Frame requestFrame,
            RequestHandler requestHandler,
            Int2ObjectHashMap<UnicastSubject<Payload>> channels,
            Int2ObjectHashMap<Subscription> cancellationSubscriptions,
            Int2ObjectHashMap<SubscriptionArbiter> inFlight) {

        UnicastSubject<Payload> channelSubject;
        final int streamId = requestFrame.getStreamId();
        synchronized(this) {
            channelSubject = channels.get(streamId);
        }
        if (channelSubject == null) {
            return child -> {
                Subscription s = new Subscription() {

                    final AtomicBoolean started = new AtomicBoolean(false);
                    final AtomicReference<Subscription> parent = new AtomicReference<>();
                    final SubscriptionArbiter arbiter = new SubscriptionArbiter();

                    @Override
                    public void request(long n) {
                        if(n <= 0) {
                            return;
                        }
                        if (started.compareAndSet(false, true)) {
                            arbiter.addTransportRequest(n);

                            // first request on this channel
                            UnicastSubject<Payload> channelRequests =
                                UnicastSubject.create((s, rn) -> {
                                    // after we are first subscribed to then send
                                    // the initial frame
                                    s.onNext(requestFrame);
                                    if (rn.intValue() > 0) {
                                        // initial requestN back to the requester (subtract 1
                                        // for the initial frame which was already sent)
                                        child.onNext(Frame.RequestN.from(streamId, rn.intValue() - 1));
                                    }
                                }, r -> {
                                    // requested
                                    child.onNext(Frame.RequestN.from(streamId, r.intValue()));
                                });
                            synchronized(Responder.this) {
                                if(channels.get(streamId) != null) {
                                    // TODO validate that this correctly defends
                                    // against this issue, this means we received a
                                    // followup request that raced and that the requester
                                    // didn't correct wait for REQUEST_N before sending
                                    // more frames
                                    RuntimeException exc = new RuntimeException(
                                        name() + " sent more than 1 requestChannel " +
                                            "frame before permitted.");
                                    child.onNext(Frame.Error.from(streamId, exc));
                                    child.onComplete();
                                    cleanup();
                                    return;
                                }
                                channels.put(streamId, channelRequests);
                            }

                            try {
                                Publisher<Payload> responses = requestHandler.handleChannel(requestFrame, channelRequests);
                                responses.subscribe(new Subscriber<Payload>() {
                                    @Override
                                    public void onSubscribe(Subscription s) {
                                        if (parent.compareAndSet(null, s)) {
                                            inFlight.put(streamId, arbiter);
                                            long n = Frame.Request.initialRequestN(requestFrame);
                                            arbiter.addApplicationRequest(n);
                                            arbiter.addApplicationProducer(s);
                                        } else {
                                            s.cancel();
                                            cleanup();
                                        }
                                    }

                                    @Override
                                    public void onNext(Payload v) {
                                        Frame nextFrame = Frame.Response.from(
                                                streamId, FrameType.NEXT, v);
                                        child.onNext(nextFrame);
                                    }

                                    @Override
                                    public void onError(Throwable t) {
                                        child.onNext(Frame.Error.from(streamId, t));
                                        child.onComplete();
                                        cleanup();
                                    }

                                    @Override
                                    public void onComplete() {
                                        Frame completeFrame = Frame.Response.from(
                                                streamId, FrameType.COMPLETE);
                                        child.onNext(completeFrame);
                                        child.onComplete();
                                        cleanup();
                                    }
                                });
                            } catch (Throwable t) {
                                child.onNext(Frame.Error.from(streamId, t));
                                child.onComplete();
                                cleanup();
                            }
                        } else {
                            arbiter.addTransportRequest(n);
                        }
                    }

                    @Override
                    public void cancel() {
                        if (!parent.compareAndSet(null, EmptySubscription.INSTANCE)) {
                            parent.get().cancel();
                            cleanup();
                        }
                    }

                    private void cleanup() {
                        synchronized(Responder.this) {
                            inFlight.remove(streamId);
                            cancellationSubscriptions.remove(streamId);
                        }
                    }

                };
                synchronized(this) {
                    cancellationSubscriptions.put(streamId, s);
                }
                child.onSubscribe(s);

            };

        } else {
            // send data to channel
            if (channelSubject.isSubscribedTo()) {
                if(Frame.Request.isRequestChannelComplete(requestFrame)) {
                    channelSubject.onComplete();
                } else {
                    // TODO this is ignoring requestN flow control (need to validate
                    // that this is legit because REQUEST_N across the wire is
                    // controlling it on the Requester side)
                    channelSubject.onNext(requestFrame);
                }
                // TODO should at least have an error message of some kind if the
                // Requester disregarded it
                return Publishers.empty();
            } else {
                // TODO should we use a BufferUntilSubscriber solution instead to
                // handle time-gap issues like this?
                // TODO validate with unit tests.
                return PublisherUtils.errorFrame(
                        streamId, new RuntimeException(name() + ": Channel unavailable"));
            }
        }
    }

    private String name() {
        if (isServer) {
            return "ServerResponder";
        } else {
            return "ClientResponder";
        }
    }

    private static class SubscriptionArbiter {
        private Subscription applicationProducer;
        private long appRequested;
        private long transportRequested;
        private long requestedToProducer;

        public void addApplicationRequest(long n) {
            synchronized(this) {
                appRequested += n;
            }
            tryRequest();
        }

        public void addApplicationProducer(Subscription s) {
            synchronized(this) {
                applicationProducer = s;
            }
            tryRequest();
        }

        public void addTransportRequest(long n) {
            synchronized(this) {
                transportRequested += n;
            }
            tryRequest();
        }

        private void tryRequest() {
            long toRequest;
            synchronized(this) {
                if(applicationProducer == null) {
                    return;
                }
                long minToRequest = Math.min(appRequested, transportRequested);
                toRequest = minToRequest - requestedToProducer;
                requestedToProducer += toRequest;
            }
            if(toRequest > 0) {
                applicationProducer.request(toRequest);
            }
        }

    }

}
