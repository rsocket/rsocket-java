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

package io.rsocket.client;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.PayloadImpl;
import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

public class LoadBalancerTest {

    @Test(timeout = 10_000L)
    public void testNeverSelectFailingFactories() throws InterruptedException {
        InetSocketAddress local0 = InetSocketAddress.createUnresolved("localhost", 7000);
        InetSocketAddress local1 = InetSocketAddress.createUnresolved("localhost", 7001);

        TestingRSocket socket = new TestingRSocket(Function.identity());
        RSocketClient failing = failingClient(local0);
        RSocketClient succeeding = succeedingFactory(socket);
        List<RSocketClient> factories = Arrays.asList(failing, succeeding);

        testBalancer(factories);
    }

    @Test(timeout = 10_000L)
    public void testNeverSelectFailingSocket() throws InterruptedException {
        InetSocketAddress local0 = InetSocketAddress.createUnresolved("localhost", 7000);
        InetSocketAddress local1 = InetSocketAddress.createUnresolved("localhost", 7001);

        TestingRSocket socket = new TestingRSocket(Function.identity());
        TestingRSocket failingSocket = new TestingRSocket(Function.identity()) {
            @Override
            public Mono<Payload> requestResponse(Payload payload) {
                return Mono.error(new RuntimeException("You shouldn't be here"));
            }

            @Override
            public double availability() {
                return 0.0;
            }
        };

        RSocketClient failing = succeedingFactory(failingSocket);
        RSocketClient succeeding = succeedingFactory(socket);
        List<RSocketClient> clients = Arrays.asList(failing, succeeding);

        testBalancer(clients);
    }

    private void testBalancer(List<RSocketClient> factories) throws InterruptedException {
        Publisher<List<RSocketClient>> src = s -> {
            s.onNext(factories);
            s.onComplete();
        };

        LoadBalancer balancer = new LoadBalancer(src);

        while (balancer.availability() == 0.0) {
            Thread.sleep(1);
        }

        for (int i = 0; i < 100; i++) {
            makeAcall(balancer);
        }
    }

    private void makeAcall(RSocket balancer) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);

        balancer.requestResponse(PayloadImpl.EMPTY).subscribe(new Subscriber<Payload>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(1L);
            }

            @Override
            public void onNext(Payload payload) {
                System.out.println("Successfully receiving a response");
            }

            @Override
            public void onError(Throwable t) {
                t.printStackTrace();
                Assert.assertTrue(false);
                latch.countDown();
            }

            @Override
            public void onComplete() {
                latch.countDown();
            }
        });

        latch.await();
    }

    private static RSocketClient succeedingFactory(RSocket socket) {
        return new RSocketClient() {
            @Override
            public Mono<RSocket> connect() {
                return Mono.just(socket);
            }

            @Override
            public double availability() {
                return 1.0;
            }

        };
    }

    private static RSocketClient failingClient(SocketAddress sa) {
        return new RSocketClient() {
            @Override
            public Mono<RSocket> connect() {
                Assert.fail();
                return null;
            }

            @Override
            public double availability() {
                return 0.0;
            }
        };
    }
}
