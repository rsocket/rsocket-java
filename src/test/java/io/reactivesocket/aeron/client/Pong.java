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
package io.reactivesocket.aeron.client;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import io.reactivesocket.ConnectionSetupHandler;
import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.Payload;
import io.reactivesocket.RequestHandler;
import io.reactivesocket.aeron.server.ReactiveSocketAeronServer;
import io.reactivesocket.exceptions.SetupException;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import uk.co.real_logic.aeron.driver.ThreadingMode;
import uk.co.real_logic.agrona.concurrent.NoOpIdleStrategy;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created by rroeser on 8/16/15.
 */
public class Pong {

    public static void startDriver() {
        ThreadingMode threadingMode = ThreadingMode.SHARED;

        boolean dedicated = Boolean.getBoolean("dedicated");

        if (true) {
            threadingMode = ThreadingMode.DEDICATED;
        }

        System.out.println("ThreadingMode => " + threadingMode);

        final uk.co.real_logic.aeron.driver.MediaDriver.Context ctx = new uk.co.real_logic.aeron.driver.MediaDriver.Context()
            .threadingMode(threadingMode)
            .conductorIdleStrategy(new NoOpIdleStrategy())
            .receiverIdleStrategy(new NoOpIdleStrategy())
            .senderIdleStrategy(new NoOpIdleStrategy());

        final uk.co.real_logic.aeron.driver.MediaDriver ignored = uk.co.real_logic.aeron.driver.MediaDriver.launchEmbedded(ctx);
    }

    public static void main(String... args) {
      //  startDriver();

        String host = System.getProperty("host", "localhost");

        System.out.println("Setting host to => " + host);

        byte[] response = new byte[1024];
        //byte[] response = new byte[1024];
        Random r = new Random();
        r.nextBytes(response);

        System.out.println("Sending data of size => " + response.length);

        final MetricRegistry metrics = new MetricRegistry();
        final Timer timer = metrics.timer("pongTimer");

        final ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build();
        reporter.start(15, TimeUnit.SECONDS);

        ReactiveSocketAeronServer server = ReactiveSocketAeronServer.create(host, 39790, new ConnectionSetupHandler() {
            @Override
            public RequestHandler apply(ConnectionSetupPayload setupPayload) throws SetupException {
                return new RequestHandler() {
                    @Override
                    public Publisher<Payload> handleRequestResponse(Payload payload) {
                        long time = System.nanoTime();

                        Publisher<Payload> publisher = new Publisher<Payload>() {
                            @Override
                            public void subscribe(Subscriber<? super Payload> s) {
                                Payload responsePayload = new Payload() {
                                    ByteBuffer data = ByteBuffer.wrap(response);
                                    ByteBuffer metadata = ByteBuffer.allocate(0);

                                    public ByteBuffer getData() {
                                        return data;
                                    }

                                    @Override
                                    public ByteBuffer getMetadata() {
                                        return metadata;
                                    }
                                };

                                s.onNext(responsePayload);

                                long diff = System.nanoTime() - time;
                                timer.update(diff, TimeUnit.NANOSECONDS);
                                s.onComplete();
                            }
                        };

                        return publisher;
                    }

                    @Override
                    public Publisher<Payload> handleRequestStream(Payload payload) {
                        return null;
                    }

                    @Override
                    public Publisher<Payload> handleSubscription(Payload payload) {
                        return null;
                    }

                    @Override
                    public Publisher<Void> handleFireAndForget(Payload payload) {
                        return null;
                    }

                    @Override
                    public Publisher<Payload> handleChannel(Payload initialPayload, Publisher<Payload> payloads) {
                        return null;
                    }

                    @Override
                    public Publisher<Void> handleMetadataPush(Payload payload) {
                        return null;
                    }
                };
            }
        });
    }

}
