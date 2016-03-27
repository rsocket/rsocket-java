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

import io.reactivesocket.rx.Completable;
import org.reactivestreams.Publisher;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Interface for a connection that supports sending requests and receiving responses
 */
public interface ReactiveSocket extends AutoCloseable {
    Publisher<Payload> requestResponse(final Payload payload);

    Publisher<Void> fireAndForget(final Payload payload);

    Publisher<Payload> requestStream(final Payload payload);

    Publisher<Payload> requestSubscription(final Payload payload);

    Publisher<Payload> requestChannel(final Publisher<Payload> payloads);

    Publisher<Void> metadataPush(final Payload payload);

    /**
     * Client check for availability to send request based on lease
     *
     * @return 0.0 to 1.0 indicating availability of sending requests
     */
    double availability();

    /**
     * Start protocol processing on the given DuplexConnection.
     */
    void start(Completable c);

    /**
     * Start and block the current thread until startup is finished.
     *
     * @throws RuntimeException
     *             of InterruptedException
     */
    default void startAndWait() {
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

    /**
     * Invoked when Requester is ready. Non-null exception if error. Null if success.
     *
     * @param c
     */
    void onRequestReady(Consumer<Throwable> c);

    /**
     * Invoked when Requester is ready with success or fail.
     *
     * @param c
     */
    void onRequestReady(Completable c);

    /**
     * Server granting new lease information to client
     *
     * Initial lease semantics are that server waits for periodic granting of leases by server side.
     *
     * @param ttl
     * @param numberOfRequests
     */
    void sendLease(int ttl, int numberOfRequests);

    void shutdown();
}
