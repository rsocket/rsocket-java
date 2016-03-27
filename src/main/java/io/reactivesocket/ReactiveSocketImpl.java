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
package io.reactivesocket;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivesocket.internal.Requester;
import io.reactivesocket.internal.Responder;
import io.reactivesocket.internal.rx.CompositeCompletable;
import io.reactivesocket.internal.rx.CompositeDisposable;
import io.reactivesocket.rx.Completable;
import io.reactivesocket.rx.Disposable;
import io.reactivesocket.rx.Observable;
import io.reactivesocket.rx.Observer;
import uk.co.real_logic.agrona.BitUtil;

import static io.reactivesocket.LeaseGovernor.NULL_LEASE_GOVERNOR;

/**
 * Interface for a connection that supports sending requests and receiving responses
 *
 * Created by servers for connections Created on demand for clients
 */
public class ReactiveSocket implements AutoCloseable {
    private static final RequestHandler EMPTY_HANDLER = new RequestHandler.Builder().build();

    private static final Consumer<Throwable> DEFAULT_ERROR_STREAM = t -> {
        // TODO should we use SLF4j, use System.err, or swallow by default?
        System.err.println("ReactiveSocket ERROR => " + t.getMessage()
            + " [Provide errorStream handler to replace this default]");
    };

    private final DuplexConnection connection;
    private final boolean isServer;
    private final Consumer<Throwable> errorStream;
    private Requester requester;
    private Responder responder;
    private final ConnectionSetupPayload requestorSetupPayload;
    private final RequestHandler clientRequestHandler;
    private final ConnectionSetupHandler responderConnectionHandler;
    private final LeaseGovernor leaseGovernor;

    private ReactiveSocket(
        DuplexConnection connection,
        boolean isServer,
        ConnectionSetupPayload serverRequestorSetupPayload,
        RequestHandler clientRequestHandler,
        ConnectionSetupHandler responderConnectionHandler,
        LeaseGovernor leaseGovernor,
        Consumer<Throwable> errorStream
    ) {
        this.connection = connection;
        this.isServer = isServer;
        this.requestorSetupPayload = serverRequestorSetupPayload;
        this.clientRequestHandler = clientRequestHandler;
        this.responderConnectionHandler = responderConnectionHandler;
        this.leaseGovernor = leaseGovernor;
        this.errorStream = errorStream;
    }

    /**
     * Create a ReactiveSocket from a client-side {@link DuplexConnection}.
     * <p>
     * A client-side connection is one that initiated the connection with a
     * server and will define the ReactiveSocket behaviors via the
     * {@link ConnectionSetupPayload} that define mime-types, leasing
     * behavior and other connection-level details.
     *
     * @param connection
     *            DuplexConnection of client-side initiated connection for
     *            the ReactiveSocket protocol to use.
     * @param setup
     *            ConnectionSetupPayload that defines mime-types and other
     *            connection behavior details.
     * @param handler
     *            (Optional) RequestHandler for responding to requests from
     *            the server. If 'null' requests will be responded to with
     *            "Not Found" errors.
     * @param errorStream
     *            (Optional) Callback for errors while processing streams
     *            over connection. If 'null' then error messages will be
     *            output to System.err.
     * @return ReactiveSocket for start, shutdown and sending requests.
     */
    public static ReactiveSocket fromClientConnection(
        DuplexConnection connection,
        ConnectionSetupPayload setup,
        RequestHandler handler,
        Consumer<Throwable> errorStream
    ) {
        if (connection == null) {
            throw new IllegalArgumentException("DuplexConnection can not be null");
        }
        if (setup == null) {
            throw new IllegalArgumentException("ConnectionSetupPayload can not be null");
        }
        final RequestHandler h = handler != null ? handler : EMPTY_HANDLER;
        Consumer<Throwable> es = errorStream != null ? errorStream : DEFAULT_ERROR_STREAM;
        return new ReactiveSocket(connection, false, setup, h, null, NULL_LEASE_GOVERNOR, es);
    }

    /**
     * Create a ReactiveSocket from a client-side {@link DuplexConnection}.
     * <p>
     * A client-side connection is one that initiated the connection with a
     * server and will define the ReactiveSocket behaviors via the
     * {@link ConnectionSetupPayload} that define mime-types, leasing
     * behavior and other connection-level details.
     * <p>
     * If this ReactiveSocket receives requests from the server it will respond
     * with "Not Found" errors.
     *
     * @param connection
     *            DuplexConnection of client-side initiated connection for the
     *            ReactiveSocket protocol to use.
     * @param setup
     *            ConnectionSetupPayload that defines mime-types and other
     *            connection behavior details.
     * @param errorStream
     *            (Optional) Callback for errors while processing streams over
     *            connection. If 'null' then error messages will be output to
     *            System.err.
     * @return ReactiveSocket for start, shutdown and sending requests.
     */
    public static ReactiveSocket fromClientConnection(
        DuplexConnection connection,
        ConnectionSetupPayload setup,
        Consumer<Throwable> errorStream
    ) {
        return fromClientConnection(connection, setup, EMPTY_HANDLER, errorStream);
    }

    public static ReactiveSocket fromClientConnection(
        DuplexConnection connection,
        ConnectionSetupPayload setup
    ) {
        return fromClientConnection(connection, setup, EMPTY_HANDLER, DEFAULT_ERROR_STREAM);
    }

    /**
     * Create a ReactiveSocket from a server-side {@link DuplexConnection}.
     * <p>
     * A server-side connection is one that accepted the connection from a
     * client and will define the ReactiveSocket behaviors via the
     * {@link ConnectionSetupPayload} that define mime-types, leasing behavior
     * and other connection-level details.
     *
     * @param connection
     * @param connectionHandler
     * @param errorConsumer
     * @return
     */
    public static ReactiveSocket fromServerConnection(
        DuplexConnection connection,
        ConnectionSetupHandler connectionHandler,
        LeaseGovernor leaseGovernor,
        Consumer<Throwable> errorConsumer
    ) {
        return new ReactiveSocket(connection, true, null, null, connectionHandler,
            leaseGovernor, errorConsumer);
    }

    public static ReactiveSocket fromServerConnection(
        DuplexConnection connection,
        ConnectionSetupHandler connectionHandler
    ) {
        return fromServerConnection(connection, connectionHandler, NULL_LEASE_GOVERNOR, t -> {});
    }

    /**
     * Initiate a request response exchange
     */
    public Publisher<Payload> requestResponse(final Payload payload) {
        assertRequester();
        return requester.requestResponse(payload);
    }

    public Publisher<Void> fireAndForget(final Payload payload) {
        assertRequester();
        return requester.fireAndForget(payload);
    }

    public Publisher<Payload> requestStream(final Payload payload) {
        assertRequester();
        return requester.requestStream(payload);
    }

    public Publisher<Payload> requestSubscription(final Payload payload) {
        assertRequester();
        return requester.requestSubscription(payload);
    }

    public Publisher<Payload> requestChannel(final Publisher<Payload> payloads) {
        assertRequester();
        return requester.requestChannel(payloads);
    }

    public Publisher<Void> metadataPush(final Payload payload) {
        assertRequester();
        return requester.metadataPush(payload);
    }

    private void assertRequester() {
        if (requester == null) {
            if (isServer) {
                if (responder == null) {
                    throw new IllegalStateException("Connection not initialized. " +
                        "Please 'start()' before submitting requests");
                } else {
                    throw new IllegalStateException("Setup not yet received from client. " +
                        "Please wait until Setup is completed, then retry.");
                }
            } else {
                throw new IllegalStateException("Connection not initialized. " +
                    "Please 'start()' before submitting requests");
            }
        }
    }

    /**
     * Client check for availability to send request based on lease
     *
     * @return 0.0 to 1.0 indicating availability of sending requests
     */
    public double availability() {
        // TODO: can happen in either direction
        assertRequester();
        return requester.availability();
    }

    /**
     * Server granting new lease information to client
     *
     * Initial lease semantics are that server waits for periodic granting of leases by server side.
     *
     * @param ttl
     * @param numberOfRequests
     */
    public void sendLease(int ttl, int numberOfRequests) {
        // TODO: can happen in either direction
        responder.sendLease(ttl, numberOfRequests);
    }

    /**
     * Start protocol processing on the given DuplexConnection.
     */
    public final void start(Completable c) {
        if (isServer) {
            responder = Responder.createServerResponder(
                new ConnectionFilter(connection, ConnectionFilter.STREAMS.FROM_CLIENT_EVEN),
                responderConnectionHandler,
                leaseGovernor,
                errorStream,
                c,
                setupPayload -> {
                    Completable two = new Completable() {
                        // wait for 2 success, or 1 error to pass on
                        AtomicInteger count = new AtomicInteger();

                        @Override
                        public void success() {
                            if (count.incrementAndGet() == 2) {
                                requesterReady.success();
                            }
                        }

                        @Override
                        public void error(Throwable e) {
                            requesterReady.error(e);
                        }
                    };
                    requester = Requester.createServerRequester(
                        new ConnectionFilter(connection, ConnectionFilter.STREAMS.FROM_SERVER_ODD),
                        setupPayload,
                        errorStream,
                        two
                    );
                    two.success(); // now that the reference is assigned in case of synchronous setup
                });
        } else {
            Completable both = new Completable() {
                // wait for 2 success, or 1 error to pass on
                AtomicInteger count = new AtomicInteger();

                @Override
                public void success() {
                    if (count.incrementAndGet() == 2) {
                        c.success();
                    }
                }

                @Override
                public void error(Throwable e) {
                    c.error(e);
                }
            };
            requester = Requester.createClientRequester(
                new ConnectionFilter(connection, ConnectionFilter.STREAMS.FROM_CLIENT_EVEN),
                requestorSetupPayload,
                errorStream,
                new Completable() {
                    @Override
                    public void success() {
                        requesterReady.success();
                        both.success();
                    }

                    @Override
                    public void error(Throwable e) {
                        requesterReady.error(e);
                        both.error(e);
                    }
                });
            responder = Responder.createClientResponder(
                new ConnectionFilter(connection, ConnectionFilter.STREAMS.FROM_SERVER_ODD),
                clientRequestHandler,
                leaseGovernor,
                errorStream,
                both
            );
        }
    }

    private final CompositeCompletable requesterReady = new CompositeCompletable();

    /**
     * Invoked when Requester is ready with success or fail.
     *
     * @param c
     */
    public final void onRequestReady(Completable c) {
        requesterReady.add(c);
    }

    /**
     * Invoked when Requester is ready. Non-null exception if error. Null if success.
     *
     * @param c
     */
    public final void onRequestReady(Consumer<Throwable> c) {
        requesterReady.add(new Completable() {
            @Override
            public void success() {
                c.accept(null);
            }

            @Override
            public void error(Throwable e) {
                c.accept(e);
            }
        });
    }

    private static class ConnectionFilter implements DuplexConnection {
        private enum STREAMS {
            FROM_CLIENT_EVEN, FROM_SERVER_ODD;
        }

        private final DuplexConnection connection;
        private final STREAMS s;

        private ConnectionFilter(DuplexConnection connection, STREAMS s) {
            this.connection = connection;
            this.s = s;
        }

        @Override
        public void close() throws IOException {
            connection.close(); // forward
        }

        @Override
        public Observable<Frame> getInput() {
            return new Observable<Frame>() {
                @Override
                public void subscribe(Observer<Frame> o) {
                    CompositeDisposable cd = new CompositeDisposable();
                    o.onSubscribe(cd);
                    connection.getInput().subscribe(new Observer<Frame>() {

                        @Override
                        public void onNext(Frame t) {
                            int streamId = t.getStreamId();
                            FrameType type = t.getType();
                            if (streamId == 0) {
                                if (FrameType.SETUP.equals(type) && s == STREAMS.FROM_CLIENT_EVEN) {
                                    o.onNext(t);
                                } else if (FrameType.LEASE.equals(type)) {
                                    o.onNext(t);
                                } else if (FrameType.ERROR.equals(type)) {
                                    // o.onNext(t); // TODO this doesn't work
                                } else if (FrameType.KEEPALIVE.equals(type)) {
                                    o.onNext(t); // TODO need tests
                                } else if (FrameType.METADATA_PUSH.equals(type)) {
                                    o.onNext(t);
                                }
                            } else if (BitUtil.isEven(streamId)) {
                                if (s == STREAMS.FROM_CLIENT_EVEN) {
                                    o.onNext(t);
                                }
                            } else {
                                if (s == STREAMS.FROM_SERVER_ODD) {
                                    o.onNext(t);
                                }
                            }
                        }

                        @Override
                        public void onError(Throwable e) {
                            o.onError(e);
                        }

                        @Override
                        public void onComplete() {
                            o.onComplete();
                        }

                        @Override
                        public void onSubscribe(Disposable d) {
                            cd.add(d);
                        }
                    });
                }
            };
        }

        @Override
        public void addOutput(Publisher<Frame> o, Completable callback) {
            connection.addOutput(o, callback);
        }

        @Override
        public void addOutput(Frame f, Completable callback) {
            connection.addOutput(f, callback);
        }

    };

    /**
     * Start and block the current thread until startup is finished.
     *
     * @throws RuntimeException
     *             of InterruptedException
     */
    public final void startAndWait() {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> err = new AtomicReference<>();
        start(new Completable() {
            @Override
            public void success() {
                latch.countDown();
            }

            @Override
            public void error(Throwable e) {
                latch.countDown();
            }
        });
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        if (err.get() != null) {
            throw new RuntimeException(err.get());
        }
    }

    @Override
    public void close() throws Exception {
        connection.close();
        leaseGovernor.unregister(responder);
        if (requester != null) {
            requester.shutdown();
        }
        if (responder != null) {
            responder.shutdown();
        }
    }

    public void shutdown() {
        try {
            close();
        } catch (Exception e) {
            throw new RuntimeException("Failed Shutdown", e);
        }
    }

    private static <T> Publisher<T> error(Throwable e) {
        return (Subscriber<? super T> s) -> {
            s.onSubscribe(new Subscription() {
                @Override
                public void request(long n) {
                    // should probably worry about n==0
                    s.onError(e);
                }

                @Override
                public void cancel() {
                    // ignoring just because
                }
            });
        };
    }
}
