/**
 * Copyright 2015 Netflix, Inc.
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
package io.reactivesocket.internal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.FrameType;
import io.reactivesocket.Payload;
import io.reactivesocket.exceptions.CancelException;
import io.reactivesocket.exceptions.Exceptions;
import io.reactivesocket.exceptions.Retryable;
import io.reactivesocket.internal.frame.RequestFrameFlyweight;
import io.reactivesocket.internal.rx.BackpressureUtils;
import io.reactivesocket.internal.rx.EmptyDisposable;
import io.reactivesocket.internal.rx.EmptySubscription;
import io.reactivesocket.rx.Completable;
import io.reactivesocket.rx.Disposable;
import io.reactivesocket.rx.Observer;
import uk.co.real_logic.agrona.collections.Int2ObjectHashMap;

/**
 * Protocol implementation abstracted over a {@link DuplexConnection}.
 * <p>
 * Concrete implementations of {@link DuplexConnection} over TCP, WebSockets, Aeron, etc can be passed to this class for protocol handling.
 */
public class Requester {

    private final static Disposable CANCELLED = new EmptyDisposable();
    private final static int KEEPALIVE_INTERVAL_MS = 1000;

    private final boolean isServer;
    private final DuplexConnection connection;
    private final Int2ObjectHashMap<UnicastSubject<Frame>> streamInputMap = new Int2ObjectHashMap<>();
    private final ConnectionSetupPayload setupPayload;
    private final Consumer<Throwable> errorStream;

    private final boolean honorLease;
    private long ttlExpiration;
    private long numberOfRemainingRequests = 0;
    private long timeOfLastKeepalive = 0;
    private int streamCount = 0; // 0 is reserved for setup, all normal messages are >= 1

    private static final long DEFAULT_BATCH = 1024;
    private static final long REQUEST_THRESHOLD = 256;

    private volatile boolean requesterStarted = false;

    private Requester(
        boolean isServer,
        DuplexConnection connection,
        ConnectionSetupPayload setupPayload,
        Consumer<Throwable> errorStream
    ) {
        this.isServer = isServer;
        this.connection = connection;
        this.setupPayload = setupPayload;
        this.errorStream = errorStream;
        if (isServer) {
            streamCount = 1; // server is odds
        } else {
            streamCount = 0; // client is even
        }

        this.honorLease = setupPayload.willClientHonorLease();
    }

    public static Requester createClientRequester(
        DuplexConnection connection,
        ConnectionSetupPayload setupPayload,
        Consumer<Throwable> errorStream,
        Completable requesterCompletable
    ) {
        Requester requester = new Requester(false, connection, setupPayload, errorStream);
        requester.start(requesterCompletable);
        return requester;
    }

    public static Requester createServerRequester(
        DuplexConnection connection,
        ConnectionSetupPayload setupPayload,
        Consumer<Throwable> errorStream,
        Completable requesterCompletable
    ) {
        Requester requester = new Requester(true, connection, setupPayload, errorStream);
        requester.start(requesterCompletable);
        return requester;
    }

    public void shutdown() {
        // TODO do something here
        System.err.println("**** Requester.shutdown => this should actually do something");
    }

    public boolean isServer() {
        return isServer;
    }

    public long timeOfLastKeepalive()
    {
        return timeOfLastKeepalive;
    }

    /**
     * Request/Response with a single message response.
     *
     * @param payload
     * @return
     */
    public Publisher<Payload> requestResponse(final Payload payload) {
        return startRequestResponse(nextStreamId(), FrameType.REQUEST_RESPONSE, payload);
    }

    /**
     * Request/Stream with a finite multi-message response followed by a
     * terminal state {@link Subscriber#onComplete()} or
     * {@link Subscriber#onError(Throwable)}.
     *
     * @param payload
     * @return
     */
    public Publisher<Payload> requestStream(final Payload payload) {
        return startStream(nextStreamId(), FrameType.REQUEST_STREAM, payload);
    }

    /**
     * Fire-and-forget without a response from the server.
     * <p>
     * The returned {@link Publisher} will emit {@link Subscriber#onComplete()}
     * or {@link Subscriber#onError(Throwable)} to represent success or failure
     * in sending from the client side, but no feedback from the server will
     * be returned.
     *
     * @param payload
     * @return
     */
    public Publisher<Void> fireAndForget(final Payload payload) {
        if (payload == null) {
            throw new IllegalStateException("Payload can not be null");
        }
        assertStarted();
        return child -> child.onSubscribe(new Subscription() {

            final AtomicBoolean started = new AtomicBoolean(false);

            @Override
            public void request(long n) {
                if (n > 0 && started.compareAndSet(false, true)) {
                    numberOfRemainingRequests--;

                    Frame fnfFrame = Frame.Request.from(
                        nextStreamId(), FrameType.FIRE_AND_FORGET, payload, 0);
                    connection.addOutput(fnfFrame, new Completable() {
                        @Override
                        public void success() {
                            child.onComplete();
                        }

                        @Override
                        public void error(Throwable e) {
                            child.onError(e);
                        }
                    });
                }
            }

            @Override
            public void cancel() {
                // nothing to cancel on a fire-and-forget
            }
        });
    }

    /**
     * Send asynchonrous Metadata Push without a response from the server.
     * <p>
     * The returned {@link Publisher} will emit {@link Subscriber#onComplete()}
     * or {@link Subscriber#onError(Throwable)} to represent success or failure
     * in sending from the client side, but no feedback from the server will be
     * returned.
     *
     * @param payload
     * @return
     */
    public Publisher<Void> metadataPush(final Payload payload) {
        if (payload == null) {
            throw new IllegalArgumentException("Payload can not be null");
        }
        assertStarted();
        return (Subscriber<? super Void> child) ->
            child.onSubscribe(new Subscription() {

                final AtomicBoolean started = new AtomicBoolean(false);

                @Override
                public void request(long n) {
                    if (n > 0 && started.compareAndSet(false, true)) {
                        numberOfRemainingRequests--;

                        Frame metadataPush = Frame.Request.from(
                            nextStreamId(), FrameType.METADATA_PUSH, payload, 0);
                        connection.addOutput(metadataPush, new Completable() {
                            @Override
                            public void success() {
                                child.onComplete();
                            }

                            @Override
                            public void error(Throwable e) {
                                child.onError(e);
                            }
                        });
                    }
                }

                @Override
                public void cancel() {
                    // nothing to cancel on a metadataPush
                }
            });
    }


    /**
     * Event subscription with an infinite multi-message response potentially
     * terminated with an {@link Subscriber#onError(Throwable)}.
     *
     * @param payload
     * @return
     */
    public Publisher<Payload> requestSubscription(final Payload payload) {
        return startStream(nextStreamId(), FrameType.REQUEST_SUBSCRIPTION, payload);
    }

    /**
     * Request/Stream with a finite multi-message response followed by a
     * terminal state {@link Subscriber#onComplete()} or
     * {@link Subscriber#onError(Throwable)}.
     *
     * @param payloadStream
     * @return
     */
    public Publisher<Payload> requestChannel(final Publisher<Payload> payloadStream) {
        return startChannel(nextStreamId(), FrameType.REQUEST_CHANNEL, payloadStream);
    }

    private void assertStarted() {
        if (!requesterStarted) {
            throw new IllegalStateException("Requester not initialized. " +
                "Please await 'start()' completion before submitting requests.");
        }
    }


    /**
     * Return availability of sending requests
     *
     * @return
     */
    public double availability() {
        if (!honorLease) {
            return 1.0;
        }
        final long now = System.currentTimeMillis();
        double available = 0.0;
        if (numberOfRemainingRequests > 0 && (now < ttlExpiration)) {
            available = 1.0;
        }
        return available;
    }

    /*
     * Using payload/payloads with null check for efficiency so I don't have to
     * allocate a Publisher for the most common case of single Payload
     */
    private Publisher<Payload> startStream(int streamId, FrameType type, Payload payload) {
        assertStarted();
        return (Subscriber<? super Payload> child) -> {
            child.onSubscribe(new Subscription() {

                final AtomicBoolean started = new AtomicBoolean(false);
                volatile StreamInputSubscriber streamInputSubscriber;
                volatile UnicastSubject<Frame> writer;
                // TODO does this need to be atomic? Can request(n) come from any thread?
                final AtomicLong requested = new AtomicLong();
                // TODO AtomicLong just so I can pass it around ... perf issue? or is there a thread-safety issue?
                final AtomicLong outstanding = new AtomicLong();

                @Override
                public void request(long n) {
                    if(n <= 0) {
                        return;
                    }
                    BackpressureUtils.getAndAddRequest(requested, n);
                    if (started.compareAndSet(false, true)) {
                        // determine initial RequestN
                        long currentN = requested.get();
                        long requestN = currentN < DEFAULT_BATCH ? currentN : DEFAULT_BATCH;
                        long threshold =
                            requestN == DEFAULT_BATCH ? REQUEST_THRESHOLD : requestN / 3;

                        // declare output to transport
                        writer = UnicastSubject.create((w, rn) -> {
                            numberOfRemainingRequests--;

                            // decrement as we request it
                            requested.addAndGet(-requestN);
                            // record how many we have requested
                            outstanding.addAndGet(requestN);

                            // when transport connects we write the request frame for this stream
                            w.onNext(Frame.Request.from(streamId, type, payload, (int)requestN));
                        });

                        // Response frames for this Stream
                        UnicastSubject<Frame> transportInputSubject = UnicastSubject.create();
                        synchronized(Requester.this) {
                            streamInputMap.put(streamId, transportInputSubject);
                        }
                        streamInputSubscriber = new StreamInputSubscriber(
                            streamId,
                            threshold,
                            outstanding,
                            requested,
                            writer,
                            child,
                            this::cancel
                        );
                        transportInputSubject.subscribe(streamInputSubscriber);

                        // connect to transport
                        connection.addOutput(writer, new Completable() {
                            @Override
                            public void success() {
                                // nothing to do onSuccess
                            }

                            @Override
                            public void error(Throwable e) {
                                child.onError(e);
                                cancel();
                            }
                        });
                    } else {
                        // propagate further requestN frames
                        long currentN = requested.get();
                        long requestThreshold =
                            REQUEST_THRESHOLD < currentN ? REQUEST_THRESHOLD : currentN / 3;
                        requestIfNecessary(
                            streamId,
                            requestThreshold,
                            currentN,
                            outstanding.get(),
                            writer,
                            requested,
                            outstanding
                        );
                    }

                }

                @Override
                public void cancel() {
                    synchronized(Requester.this) {
                        streamInputMap.remove(streamId);
                    }
                    if (!streamInputSubscriber.terminated.get()) {
                        writer.onNext(Frame.Cancel.from(streamId));
                    }
                    streamInputSubscriber.parentSubscription.cancel();
                }

            });
        };
    }

    /*
     * Using payload/payloads with null check for efficiency so I don't have to
     * allocate a Publisher for the most common case of single Payload
     */
    private Publisher<Payload> startChannel(
        int streamId,
        FrameType type,
        Publisher<Payload> payloads
    ) {
        if (payloads == null) {
            throw new IllegalStateException("Both payload and payloads can not be null");
        }
        assertStarted();
        return (Subscriber<? super Payload> child) -> {
            child.onSubscribe(new Subscription() {

                AtomicBoolean started = new AtomicBoolean(false);
                volatile StreamInputSubscriber streamInputSubscriber;
                volatile UnicastSubject<Frame> writer;
                final AtomicReference<Subscription> payloadsSubscription = new AtomicReference<>();
                // TODO does this need to be atomic? Can request(n) come from any thread?
                final AtomicLong requested = new AtomicLong();
                // TODO AtomicLong just so I can pass it around ... perf issue? or is there a thread-safety issue?
                final AtomicLong outstanding = new AtomicLong();

                @Override
                public void request(long n) {
                    if(n <= 0) {
                        return;
                    }
                    BackpressureUtils.getAndAddRequest(requested, n);
                    if (started.compareAndSet(false, true)) {
                        // determine initial RequestN
                        long currentN = requested.get();
                        final long requestN = currentN < DEFAULT_BATCH ? currentN : DEFAULT_BATCH;
                        // threshold
                        final long threshold =
                            requestN == DEFAULT_BATCH ? REQUEST_THRESHOLD : requestN / 3;

                        // declare output to transport
                        writer = UnicastSubject.create((w, rn) -> {
                            numberOfRemainingRequests--;
                            // decrement as we request it
                            requested.addAndGet(-requestN);
                            // record how many we have requested
                            outstanding.addAndGet(requestN);

                            connection.addOutput(new Publisher<Frame>() {
                                @Override
                                public void subscribe(Subscriber<? super Frame> transport) {
                                    transport.onSubscribe(new Subscription() {

                                        final AtomicBoolean started = new AtomicBoolean(false);
                                        @Override
                                        public void request(long n) {
                                            if(n <= 0) {
                                                return;
                                            }
                                            if(started.compareAndSet(false, true)) {
                                                payloads.subscribe(new Subscriber<Payload>() {

                                                    @Override
                                                    public void onSubscribe(Subscription s) {
                                                        if (!payloadsSubscription.compareAndSet(null, s)) {
                                                            // we are already unsubscribed
                                                            s.cancel();
                                                        } else {
                                                            // we always start with 1 to initiate
                                                            // requestChannel, then wait for REQUEST_N
                                                            // from Responder to send more
                                                            s.request(1);
                                                        }
                                                    }

                                                    // onNext is serialized by contract so this is
                                                    // okay as non-volatile primitive
                                                    boolean isInitialRequest = true;

                                                    @Override
                                                    public void onNext(Payload p) {
                                                        if(isInitialRequest) {
                                                            isInitialRequest = false;
                                                            Frame f = Frame.Request.from(
                                                                streamId, type, p, (int)requestN);
                                                            transport.onNext(f);
                                                        } else {
                                                            Frame f = Frame.Request.from(
                                                                streamId, type, p, 0);
                                                            transport.onNext(f);
                                                        }
                                                    }

                                                    @Override
                                                    public void onError(Throwable t) {
                                                        // TODO validate with unit tests
                                                        RuntimeException exc = new RuntimeException(
                                                            "Error received from request stream.", t);
                                                        transport.onError(exc);
                                                        child.onError(exc);
                                                        cancel();
                                                    }

                                                    @Override
                                                    public void onComplete() {
                                                        Frame f = Frame.Request.from(
                                                            streamId,
                                                            FrameType.REQUEST_CHANNEL,
                                                            RequestFrameFlyweight.FLAGS_REQUEST_CHANNEL_C
                                                        );
                                                        transport.onNext(f);
                                                        transport.onComplete();
                                                    }

                                                });
                                            } else {
                                                // TODO we need to compose this requestN from
                                                // transport with the remote REQUEST_N
                                            }

                                        }

                                        @Override
                                        public void cancel() {}
                                    });
                                }
                            }, new Completable() {
                                @Override
                                public void success() {
                                    // nothing to do onSuccess
                                }

                                @Override
                                public void error(Throwable e) {
                                    child.onError(e);
                                    cancel();
                                }
                            });

                        });

                        // Response frames for this Stream
                        UnicastSubject<Frame> transportInputSubject = UnicastSubject.create();
                        synchronized(Requester.this) {
                            streamInputMap.put(streamId, transportInputSubject);
                        }
                        streamInputSubscriber = new StreamInputSubscriber(
                            streamId,
                            threshold,
                            outstanding,
                            requested,
                            writer,
                            child,
                            payloadsSubscription,
                            this::cancel
                        );
                        transportInputSubject.subscribe(streamInputSubscriber);

                        // connect to transport
                        connection.addOutput(writer, new Completable() {
                            @Override
                            public void success() {
                                // nothing to do onSuccess
                            }

                            @Override
                            public void error(Throwable e) {
                                child.onError(e);
                                if (!(e instanceof Retryable)) {
                                    cancel();
                                }
                            }
                        });
                    } else {
                        // propagate further requestN frames
                        long currentN = requested.get();
                        long requestThreshold =
                            REQUEST_THRESHOLD < currentN ? REQUEST_THRESHOLD : currentN / 3;
                        requestIfNecessary(
                            streamId,
                            requestThreshold,
                            currentN,
                            outstanding.get(),
                            writer,
                            requested,
                            outstanding
                        );
                    }
                }

                @Override
                public void cancel() {
                    synchronized(Requester.this) {
                        streamInputMap.remove(streamId);
                    }
                    if (!streamInputSubscriber.terminated.get()) {
                        writer.onNext(Frame.Cancel.from(streamId));
                    }
                    streamInputSubscriber.parentSubscription.cancel();
                    if (payloadsSubscription != null) {
                        if (!payloadsSubscription.compareAndSet(null, EmptySubscription.INSTANCE)) {
                            // unsubscribe it if it already exists
                            payloadsSubscription.get().cancel();
                        }
                    }
                }

            });
        };
    }

    /*
     * Special-cased for performance reasons (achieved 20-30% throughput
     * increase over using startStream for request/response)
     */
    private Publisher<Payload> startRequestResponse(int streamId, FrameType type, Payload payload) {
        if (payload == null) {
            throw new IllegalStateException("Both payload and payloads can not be null");
        }
        assertStarted();
        return (Subscriber<? super Payload> child) -> {
            child.onSubscribe(new Subscription() {

                final AtomicBoolean started = new AtomicBoolean(false);
                volatile StreamInputSubscriber streamInputSubscriber;
                volatile UnicastSubject<Frame> writer;

                @Override
                public void request(long n) {
                    if (n > 0 && started.compareAndSet(false, true)) {
                        // Response frames for this Stream
                        UnicastSubject<Frame> transportInputSubject = UnicastSubject.create();
                        synchronized(Requester.this) {
                            streamInputMap.put(streamId, transportInputSubject);
                        }
                        streamInputSubscriber = new StreamInputSubscriber(
                            streamId,
                            0,
                            null,
                            null,
                            writer,
                            child,
                            this::cancel
                        );
                        transportInputSubject.subscribe(streamInputSubscriber);

                        Frame requestFrame = Frame.Request.from(streamId, type, payload, 1);
                        // connect to transport
                        connection.addOutput(requestFrame, new Completable() {
                            @Override
                            public void success() {
                                // nothing to do onSuccess
                            }

                            @Override
                            public void error(Throwable e) {
                                child.onError(e);
                                cancel();
                            }
                        });
                    }
                }

                @Override
                public void cancel() {
                    if (!streamInputSubscriber.terminated.get()) {
                        Frame cancelFrame = Frame.Cancel.from(streamId);
                        connection.addOutput(cancelFrame, new Completable() {
                            @Override
                            public void success() {
                                // nothing to do onSuccess
                            }

                            @Override
                            public void error(Throwable e) {
                                child.onError(e);
                            }
                        });
                    }
                    synchronized(Requester.this) {
                        streamInputMap.remove(streamId);
                    }
                    streamInputSubscriber.parentSubscription.cancel();
                }
            });
        };
    }

    private final static class StreamInputSubscriber implements Subscriber<Frame> {
        final AtomicBoolean terminated = new AtomicBoolean(false);
        volatile Subscription parentSubscription;

        private final int streamId;
        private final long requestThreshold;
        private final AtomicLong outstandingRequests;
        private final AtomicLong requested;
        private final UnicastSubject<Frame> writer;
        private final Subscriber<? super Payload> child;
        private final Runnable cancelAction;
        private final AtomicReference<Subscription> requestStreamSubscription;

        public StreamInputSubscriber(
            int streamId,
            long threshold,
            AtomicLong outstanding,
            AtomicLong requested,
            UnicastSubject<Frame> writer,
            Subscriber<? super Payload> child,
            Runnable cancelAction
        ) {
            this.streamId = streamId;
            this.requestThreshold = threshold;
            this.requested = requested;
            this.outstandingRequests = outstanding;
            this.writer = writer;
            this.child = child;
            this.cancelAction = cancelAction;
            this.requestStreamSubscription = null;
        }

        public StreamInputSubscriber(
            int streamId,
            long threshold,
            AtomicLong outstanding,
            AtomicLong requested,
            UnicastSubject<Frame> writer,
            Subscriber<? super Payload> child,
            AtomicReference<Subscription> requestStreamSubscription,
            Runnable cancelAction
        ) {
            this.streamId = streamId;
            this.requestThreshold = threshold;
            this.requested = requested;
            this.outstandingRequests = outstanding;
            this.writer = writer;
            this.child = child;
            this.cancelAction = cancelAction;
            this.requestStreamSubscription = requestStreamSubscription;
        }

        @Override
        public void onSubscribe(Subscription s) {
            this.parentSubscription = s;
            // no backpressure to transport (we will only receive what we've asked for already)
            s.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(Frame frame) {
            FrameType type = frame.getType();
            // convert ERROR messages into terminal events
            if (type == FrameType.NEXT_COMPLETE) {
                terminated.set(true);
                child.onNext(frame);
                onComplete();
                cancel();
            } else if (type == FrameType.NEXT) {
                child.onNext(frame);
                long currentOutstanding = outstandingRequests.decrementAndGet();
                requestIfNecessary(streamId, requestThreshold, requested.get(),
                    currentOutstanding, writer, requested, outstandingRequests);
            } else if (type == FrameType.REQUEST_N) {
                if(requestStreamSubscription != null) {
                    Subscription s = requestStreamSubscription.get();
                    if(s != null) {
                        s.request(Frame.RequestN.requestN(frame));
                    } else {
                        // TODO can this ever be null?
                        System.err.println(
                            "ReactiveSocket Requester DEBUG: requestStreamSubscription is null");
                    }
                    return;
                }
                // TODO should we do anything if we don't find the stream? emitting an error
                // is risky as the responder could have terminated and cleaned up already
            } else if (type == FrameType.COMPLETE) {
                terminated.set(true);
                onComplete();
                cancel();
            } else if (type == FrameType.ERROR) {
                terminated.set(true);
                final ByteBuffer byteBuffer = frame.getData();
                String errorMessage = getByteBufferAsString(byteBuffer);
                onError(new RuntimeException(errorMessage));
                cancel();
            } else {
                onError(new RuntimeException("Unexpected FrameType: " + frame.getType()));
                cancel();
            }
        }

        @Override
        public void onError(Throwable t) {
            terminated.set(true);
            child.onError(t);
        }

        @Override
        public void onComplete() {
            terminated.set(true);
            child.onComplete();
        }

        private void cancel() {
            cancelAction.run();
        }
    }

    private static void requestIfNecessary(
        int streamId,
        long requestThreshold,
        long currentN,
        long currentOutstanding,
        UnicastSubject<Frame> writer,
        AtomicLong requested,
        AtomicLong outstanding
    ) {
        if(currentOutstanding <= requestThreshold) {
            long batchSize = DEFAULT_BATCH - currentOutstanding;
            final long requestN = currentN < batchSize ? currentN : batchSize;

            if (requestN > 0) {
                // decrement as we request it
                requested.addAndGet(-requestN);
                // record how many we have requested
                outstanding.addAndGet(requestN);

                writer.onNext(Frame.RequestN.from(streamId, (int)requestN));
            }
        }
    }

    private int nextStreamId() {
        return streamCount += 2; // go by two since server is odd, client is even
    }

    private void start(Completable onComplete) {
        AtomicReference<Disposable> connectionSubscription = new AtomicReference<>();
        // get input from responder->requestor for responses
        connection.getInput().subscribe(new Observer<Frame>() {
            public void onSubscribe(Disposable d) {
                if (connectionSubscription.compareAndSet(null, d)) {
                    if(isServer) {
                        requesterStarted = true;
                        onComplete.success();
                    } else {
                        // now that we are connected, send SETUP frame
                        // (asynchronously, other messages can continue being written after this)
                        Frame setupFrame = Frame.Setup.from(
                            setupPayload.getFlags(),
                            KEEPALIVE_INTERVAL_MS,
                            0,
                            setupPayload.metadataMimeType(),
                            setupPayload.dataMimeType(),
                            setupPayload
                        );
                        connection.addOutput(setupFrame,
                            new Completable() {
                                @Override
                                public void success() {
                                    requesterStarted = true;
                                    onComplete.success();
                                }

                                @Override
                                public void error(Throwable e) {
                                    onComplete.error(e);
                                    tearDown(e);
                                }
                            });

                        Publisher<Frame> keepaliveTicker =
                            PublisherUtils.keepaliveTicker(KEEPALIVE_INTERVAL_MS, TimeUnit.MILLISECONDS);
                        connection.addOutput(keepaliveTicker,
                            new Completable() {
                                public void success() {}

                                public void error(Throwable e) {
                                    onComplete.error(e);
                                    tearDown(e);
                                }
                            }
                        );
                    }
                } else {
                    // means we already were cancelled
                    d.dispose();
                    onComplete.error(new CancelException("Connection Is Already Cancelled"));
                }
            }

            private void tearDown(Throwable e) {
                onError(e);
            }

            public void onNext(Frame frame) {
                int streamId = frame.getStreamId();
                if (streamId == 0) {
                    if (FrameType.ERROR.equals(frame.getType())) {
                        final Throwable throwable = Exceptions.from(frame);
                        onError(throwable);
                    } else if (FrameType.LEASE.equals(frame.getType()) && honorLease) {
                        numberOfRemainingRequests = Frame.Lease.numberOfRequests(frame);
                        final long now = System.currentTimeMillis();
                        final int ttl = Frame.Lease.ttl(frame);
                        if (ttl == Integer.MAX_VALUE) {
                            // Integer.MAX_VALUE represents infinity
                            ttlExpiration = Long.MAX_VALUE;
                        } else {
                            ttlExpiration = now + ttl;
                        }
                    } else if (FrameType.KEEPALIVE.equals(frame.getType())) {
                        timeOfLastKeepalive = System.currentTimeMillis();
                    } else {
                        onError(new RuntimeException(
                            "Received unexpected message type on stream 0: " + frame.getType().name()));
                    }
                } else {
                    UnicastSubject<Frame> streamSubject;
                    synchronized (Requester.this) {
                        streamSubject = streamInputMap.get(streamId);
                    }
                    if (streamSubject == null) {
                        if (streamId <= streamCount) {
                            // receiving a frame after a given stream has been cancelled/completed,
                            // so ignore (cancellation is async so there is a race condition)
                            return;
                        } else {
                            // message for stream that has never existed, we have a problem with
                            // the overall connection and must tear down
                            if (frame.getType() == FrameType.ERROR) {
                                String errorMessage = getByteBufferAsString(frame.getData());
                                onError(new RuntimeException(
                                    "Received error for non-existent stream: "
                                        + streamId + " Message: " + errorMessage));
                            } else {
                                onError(new RuntimeException(
                                    "Received message for non-existent stream: " + streamId));
                            }
                        }
                    } else {
                        streamSubject.onNext(frame);
                    }
                }
            }

            public void onError(Throwable t) {
                Collection<UnicastSubject<Frame>> subjects = null;
                synchronized (Requester.this) {
                    subjects = streamInputMap.values();
                }
                subjects.forEach(subject -> subject.onError(t));
                // TODO: iterate over responder side and destroy world
                errorStream.accept(t);
                cancel();
            }

            public void onComplete() {
                Collection<UnicastSubject<Frame>> subjects = null;
                synchronized (Requester.this) {
                    subjects = streamInputMap.values();
                }
                subjects.forEach(UnicastSubject::onComplete);
                cancel();
            }

            public void cancel() { // TODO this isn't used ... is it supposed to be?
                if (!connectionSubscription.compareAndSet(null, CANCELLED)) {
                    // cancel the one that was there if we failed to set the sentinel
                    connectionSubscription.get().dispose();
                    try {
                        connection.close();
                    } catch (IOException e) {
                        errorStream.accept(e);
                    }
                }
            }
        });
    }

    private static String getByteBufferAsString(ByteBuffer bb) {
        final byte[] bytes = new byte[bb.capacity()];
        bb.get(bytes);
        return new String(bytes, Charset.forName("UTF-8"));
    }
}
