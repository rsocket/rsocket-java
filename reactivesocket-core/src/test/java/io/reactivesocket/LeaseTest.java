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

import io.reactivesocket.internal.Responder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.core.test.TestSubscriber;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.reactivesocket.TestUtil.byteToString;
import static io.reactivesocket.TestUtil.utf8EncodedPayload;
import static io.reactivesocket.ConnectionSetupPayload.HONOR_LEASE;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LeaseTest {
    private TestConnection clientConnection;
    private ReactiveSocket socketServer;
    private ReactiveSocket socketClient;
    private TestingLeaseGovernor leaseGovernor;

    private class TestingLeaseGovernor implements LeaseGovernor {
        private volatile Responder responder;
        private volatile long ttlExpiration;
        private volatile int grantedTickets;
        private CountDownLatch latch = new CountDownLatch(1);

        @Override
        public synchronized void register(Responder responder) {
            this.responder = responder;
            latch.countDown();
        }

        @Override
        public synchronized void unregister(Responder responder) {
            this.responder = null;
        }

        @Override
        public synchronized boolean accept(Responder responder, Frame frame) {
            boolean valid = grantedTickets > 0
                && ttlExpiration >= System.currentTimeMillis();
            grantedTickets--;
            return valid;
        }

        public synchronized void distribute(int ttlMs, int tickets) {
            if (responder == null) {
                throw new IllegalStateException("responder is null");
            }
            ttlExpiration = System.currentTimeMillis() + ttlMs;
            grantedTickets = tickets;
            responder.sendLease(ttlMs, tickets);
        }
    }

    @Before
    public void setup() throws InterruptedException {
        TestConnection serverConnection = new TestConnection();
        clientConnection = new TestConnection();
        clientConnection.connectToServerConnection(serverConnection);
        leaseGovernor = new TestingLeaseGovernor();

        socketServer = DefaultReactiveSocket.fromServerConnection(
            serverConnection, (setup, rs) -> new RequestHandler() {

                @Override
                public Publisher<Payload> handleRequestResponse(Payload payload) {
                    return Flux.just(utf8EncodedPayload("hello world", null));
                }

                @Override
                public Publisher<Payload> handleRequestStream(Payload payload) {
                    return
                        Flux.range(0, 100)
                            .map(i -> "hello world " + i)
                            .map(n -> utf8EncodedPayload(n, null)
                    );
                }

                @Override
                public Publisher<Payload> handleSubscription(Payload payload) {
                    return Flux.interval(1, Schedulers.newTimer("timer", 1))
                        .map(i -> "subscription " + i)
                        .map(n -> utf8EncodedPayload(n, null));
                }

                @Override
                public Publisher<Void> handleFireAndForget(Payload payload) {
                    return Flux.empty();
                }

                /**
                 * Use Payload.metadata for routing
                 */
                @Override
                public Publisher<Payload> handleChannel(
                    Payload initialPayload, Publisher<Payload> inputs
                ) {
                    return Flux.from(inputs).map(p ->
                        utf8EncodedPayload(byteToString(p.getData()) + "_echo", null));
                }

                @Override
                public Publisher<Void> handleMetadataPush(Payload payload) {
                    throw new IllegalStateException(
                        "TestingLeaseGovernor.handleMetadataPush is not implemented!");
                }
            }, leaseGovernor, t -> {});

        socketClient = DefaultReactiveSocket.fromClientConnection(
            clientConnection,
            ConnectionSetupPayload.create("UTF-8", "UTF-8", HONOR_LEASE)
        );

        // start both the server and client and monitor for errors
        LatchedCompletable lc = new LatchedCompletable(2);
        socketServer.start(lc);
        socketClient.start(lc);
        if(!lc.await(3000, TimeUnit.MILLISECONDS)) {
        	throw new RuntimeException("Timed out waiting for startup");
        }
    }

    @After
    public void shutdown() {
        socketServer.shutdown();
        socketClient.shutdown();
    }

    @Test(timeout=2000)
    public void testWriteWithoutLease() throws InterruptedException {
        // initially client doesn't have any availability
        assertTrue(socketClient.availability() == 0.0);
        leaseGovernor.latch.await();
        assertTrue(socketClient.availability() == 0.0);

        // the first call will fail without a valid lease
        Publisher<Payload> response0 = socketClient.requestResponse(
            TestUtil.utf8EncodedPayload("hello", null));
        TestSubscriber<Payload> ts0 = TestSubscriber.create();
        response0.subscribe(ts0);
        ts0.await(Duration.ofMillis(500));

        // send a Lease(10 sec, 1 message), and wait for the availability on the client side
        leaseGovernor.distribute(10_000, 1);
        awaitSocketAvailabilityChange(socketClient, 1.0, 10, TimeUnit.SECONDS);

        // the second call will succeed
        Publisher<Payload> response1 = socketClient.requestResponse(
            TestUtil.utf8EncodedPayload("hello", null));
        TestSubscriber<Payload> ts1 = TestSubscriber.create();
        response1.subscribe(ts1);
        ts1.await(Duration.ofMillis(500));
        ts1.assertNoError();
        ts1.assertValuesWith(value ->
            assertEquals(TestUtil.utf8EncodedPayload("hello world", null), value)
        );

        // the client consumed all its ticket, next call will fail
        // (even though the window is still ok)
        Publisher<Payload> response2 = socketClient.requestResponse(
            TestUtil.utf8EncodedPayload("hello", null));
        TestSubscriber<Payload> ts2 = TestSubscriber.create();
        response2.subscribe(ts2);
        ts2.await(Duration.ofMillis(500));
        ts2.assertError(RuntimeException.class);
    }

    @Test(timeout=2000)
    public void testLeaseOverwrite() throws InterruptedException {

        assertTrue(socketClient.availability() == 0.0);
        leaseGovernor.latch.await();
        assertTrue(socketClient.availability() == 0.0);

        leaseGovernor.distribute(10_000, 100);
        awaitSocketAvailabilityChange(socketClient, 1.0, 10, TimeUnit.SECONDS);

        leaseGovernor.distribute(10_000, 0);
        awaitSocketAvailabilityChange(socketClient, 0.0, 10, TimeUnit.SECONDS);
    }

    private void awaitSocketAvailabilityChange(
        ReactiveSocket socket,
        double expected,
        long timeout,
        TimeUnit unit
    ) throws InterruptedException {
        long waitTimeMs = 1L;
        long startTime = System.nanoTime();
        long timeoutNanos = TimeUnit.NANOSECONDS.convert(timeout, unit);

        while (socket.availability() != expected) {
            Thread.sleep(waitTimeMs);
            waitTimeMs = Math.min(waitTimeMs * 2, 1000L);
            final long elapsedNanos = System.nanoTime() - startTime;
            if (elapsedNanos > timeoutNanos) {
                throw new IllegalStateException("Timeout while waiting for socket availability");
            }
        }
    }
    
}
