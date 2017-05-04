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

package io.rsocket;

import io.rsocket.client.KeepAliveProvider;
import io.rsocket.client.RSocketClient;
import io.rsocket.client.SetupProvider;
import io.rsocket.lease.DisabledLeaseAcceptingSocket;
import io.rsocket.lease.LeaseEnforcingSocket;
import io.rsocket.perfutil.TestDuplexConnection;
import io.rsocket.server.RSocketServer;
import io.rsocket.transport.TransportServer;
import io.rsocket.util.PayloadImpl;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


@BenchmarkMode(Mode.Throughput)
@Fork(value = 1, jvmArgsAppend = { "-XX:+UnlockCommercialFeatures", "-XX:+FlightRecorder" })
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@State(Scope.Thread)
public class RSocketPerf {

    @Benchmark
    public void requestResponseHello(Input input) {
        try {
            input.client.requestResponse(Input.HELLO_PAYLOAD).subscribe(input.blackHoleSubscriber);
        }  catch (Throwable t) {
            t.printStackTrace();
        }
    }

    @Benchmark
    public void requestStreamHello1000(Input input) {
        try {
            input.client.requestStream(Input.HELLO_PAYLOAD).subscribe(input.blackHoleSubscriber);
        }  catch (Throwable t) {
            t.printStackTrace();
        }
    }

    @Benchmark
    public void fireAndForgetHello(Input input) {
        // this is synchronous so we don't need to use a CountdownLatch to wait
        input.client.fireAndForget(Input.HELLO_PAYLOAD).subscribe(input.blackHoleSubscriber);
    }

    @State(Scope.Thread)
    public static class Input {
        /**
         * Use to consume values when the test needs to return more than a single value.
         */
        public Blackhole bh;

        static final ByteBuffer HELLO = ByteBuffer.wrap("HELLO".getBytes(StandardCharsets.UTF_8));

        static final Payload HELLO_PAYLOAD = new PayloadImpl(HELLO);

        static final DirectProcessor<Frame> clientReceive = DirectProcessor.create();
        static final DirectProcessor<Frame> serverReceive = DirectProcessor.create();

        static final TestDuplexConnection clientConnection = new TestDuplexConnection(serverReceive, clientReceive);
        static final TestDuplexConnection serverConnection = new TestDuplexConnection(clientReceive, serverReceive);

        static final Object server = RSocketServer.create(new TransportServer() {
            @Override
            public StartedServer start(ConnectionAcceptor acceptor) {
                acceptor.apply(serverConnection).subscribe();
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
            .start(new RSocketServer.SocketAcceptor() {
            @Override
            public LeaseEnforcingSocket accept(ConnectionSetupPayload setup, RSocket sendingSocket) {

                return new DisabledLeaseAcceptingSocket(new RSocket() {
                    @Override
                    public Mono<Void> fireAndForget(Payload payload) {
                        return Mono.empty();
                    }

                    @Override
                    public Mono<Payload> requestResponse(Payload payload) {
                        return Mono.just(HELLO_PAYLOAD);
                    }

                    @Override
                    public Flux<Payload> requestStream(Payload payload) {
                        return Flux.range(1, 1_000).flatMap(i -> requestResponse(payload));
                    }

                    @Override
                    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
                        return Flux.empty();
                    }

                    @Override
                    public Mono<Void> metadataPush(Payload payload) {
                        return Mono.empty();
                    }

                    @Override
                    public Mono<Void> close() {
                        return Mono.empty();
                    }

                    @Override
                    public Mono<Void> onClose() {
                        return Mono.empty();
                    }
                });
            }
        });

        Subscriber blackHoleSubscriber;

        RSocket client;

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
            RSocketClient reactiveSocketClient = RSocketClient.create(() -> Mono.just(clientConnection), setupProvider);

            CountDownLatch latch = new CountDownLatch(1);
            reactiveSocketClient.connect()
                .doOnNext(r -> this.client = r)
                .doFinally(signalType -> latch.countDown())
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
