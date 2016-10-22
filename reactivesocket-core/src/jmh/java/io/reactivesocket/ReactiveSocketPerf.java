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
import io.reactivesocket.client.ReactiveSocketClient;
import io.reactivesocket.client.SetupProvider;
import io.reactivesocket.lease.DisabledLeaseAcceptingSocket;
import io.reactivesocket.lease.LeaseEnforcingSocket;
import io.reactivesocket.perfutil.TestDuplexConnection;
import io.reactivesocket.reactivestreams.extensions.Px;
import io.reactivesocket.server.ReactiveSocketServer;
import io.reactivesocket.transport.TransportServer;
import io.reactivex.processors.PublishProcessor;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class ReactiveSocketPerf {

    @Benchmark
    public void requestResponseHello(Input input) {
        try {
            input.client.requestResponse(Input.HELLO_PAYLOAD).subscribe(input.blackHoleSubscriber);
        }  catch (Throwable t) {
            t.printStackTrace();
        }
    }

    //@Benchmark
    public void requestStreamHello1000(Input input) {
        // this is synchronous so we don't need to use a CountdownLatch to wait
        //Input.client.requestStream(Input.HELLO_PAYLOAD).subscribe(input.blackholeConsumer);
    }

    //@Benchmark
    public void fireAndForgetHello(Input input) {
        // this is synchronous so we don't need to use a CountdownLatch to wait
        //Input.client.fireAndForget(Input.HELLO_PAYLOAD).subscribe(input.voidBlackholeConsumer);
    }

    @State(Scope.Thread)
    public static class Input {
        /**
         * Use to consume values when the test needs to return more than a single value.
         */
        public Blackhole bh;

        static final ByteBuffer HELLO = ByteBuffer.wrap("HELLO".getBytes(StandardCharsets.UTF_8));
        static final ByteBuffer HELLO_WORLD = ByteBuffer.wrap("HELLO_WORLD".getBytes(StandardCharsets.UTF_8));
        static final ByteBuffer EMPTY = ByteBuffer.allocate(0);

        static final Payload HELLO_PAYLOAD = new Payload() {

            @Override
            public ByteBuffer getMetadata() {
                return EMPTY;
            }

            @Override
            public ByteBuffer getData() {
                HELLO.position(0);
                return HELLO;
            }
        };

        static final Payload HELLO_WORLD_PAYLOAD = new Payload() {

            @Override
            public ByteBuffer getMetadata() {
                return EMPTY;
            }

            @Override
            public ByteBuffer getData() {
                HELLO_WORLD.position(0);
                return HELLO_WORLD;
            }
        };


        static final PublishProcessor<Frame> clientReceive = PublishProcessor.create();
        static final PublishProcessor<Frame> serverReceive = PublishProcessor.create();

        static final TestDuplexConnection clientConnection = new TestDuplexConnection(serverReceive, clientReceive);
        static final TestDuplexConnection serverConnection = new TestDuplexConnection(clientReceive, serverReceive);

        static final Object server = ReactiveSocketServer.create(new TransportServer() {
            @Override
            public StartedServer start(ConnectionAcceptor acceptor) {
                Px.from(acceptor.apply(serverConnection)).subscribe();
                return new StartedServer() {
                    @Override
                    public SocketAddress getServerAddress() {
                        return InetSocketAddress.createUnresolved("localhost", 1234);
                    }

                    @Override
                    public int getServerPort() {
                        return 1234;
                    }

                    @Override
                    public void awaitShutdown() {

                    }

                    @Override
                    public void awaitShutdown(long duration, TimeUnit durationUnit) {

                    }

                    @Override
                    public void shutdown() {

                    }
                };
            }
        })
            .start(new ReactiveSocketServer.SocketAcceptor() {
            @Override
            public LeaseEnforcingSocket accept(ConnectionSetupPayload setup, ReactiveSocket sendingSocket) {

                return new DisabledLeaseAcceptingSocket(new ReactiveSocket() {
                    @Override
                    public Publisher<Void> fireAndForget(Payload payload) {
                        return Px.empty();
                    }

                    @Override
                    public Publisher<Payload> requestResponse(Payload payload) {
                        return Px.just(HELLO_PAYLOAD);
                    }

                    @Override
                    public Publisher<Payload> requestStream(Payload payload) {
                        return null;
                    }

                    @Override
                    public Publisher<Payload> requestSubscription(Payload payload) {
                        return null;
                    }

                    @Override
                    public Publisher<Payload> requestChannel(Publisher<Payload> payloads) {
                        return null;
                    }

                    @Override
                    public Publisher<Void> metadataPush(Payload payload) {
                        return null;
                    }

                    @Override
                    public Publisher<Void> close() {
                        return null;
                    }

                    @Override
                    public Publisher<Void> onClose() {
                        return null;
                    }
                });
            }
        });

        Subscriber blackHoleSubscriber;

        ReactiveSocket client;

        @Setup
        public void setup(Blackhole bh) {
            blackHoleSubscriber = new Subscriber() {
                @Override
                public void onSubscribe(Subscription s) {
                    s.request(Long.MAX_VALUE);
                }

                @Override
                public void onNext(Object o) {
                    bh.consume(o);
                }

                @Override
                public void onError(Throwable t) {
                    t.printStackTrace();
                }

                @Override
                public void onComplete() {

                }
            };

            SetupProvider setupProvider = SetupProvider.keepAlive(KeepAliveProvider.never()).disableLease();
            ReactiveSocketClient reactiveSocketClient = ReactiveSocketClient.create(() -> Px.just(clientConnection), setupProvider);

            CountDownLatch latch = new CountDownLatch(1);
            Px
                .from(reactiveSocketClient.connect())
                .doOnNext(r -> this.client = r)
                .doOnComplete(latch::countDown)
                .subscribe();

            try {
                latch.await();
            } catch (Throwable t) {
                t.printStackTrace();
            }

            this.bh = bh;
        }
    }

}
