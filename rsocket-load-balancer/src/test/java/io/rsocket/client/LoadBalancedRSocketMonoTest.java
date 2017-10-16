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

import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.client.filter.RSocketSupplier;
import io.rsocket.util.PayloadImpl;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class LoadBalancedRSocketMonoTest {
  private static final Duration TIMEOUT = ofSeconds(10);

  @Test
  public void testNeverSelectFailingFactories() {
    assertTimeout(
        TIMEOUT,
        () -> {
          InetSocketAddress local0 = InetSocketAddress.createUnresolved("localhost", 7000);
          InetSocketAddress local1 = InetSocketAddress.createUnresolved("localhost", 7001);

          TestingRSocket socket = new TestingRSocket(Function.identity());
          RSocketSupplier failing = failingClient(local0);
          RSocketSupplier succeeding = succeedingFactory(socket);
          List<RSocketSupplier> factories = Arrays.asList(failing, succeeding);

          testBalancer(factories);
        });
  }

  @Test
  public void testNeverSelectFailingSocket() {
    assertTimeout(
        TIMEOUT,
        () -> {
          InetSocketAddress local0 = InetSocketAddress.createUnresolved("localhost", 7000);
          InetSocketAddress local1 = InetSocketAddress.createUnresolved("localhost", 7001);

          TestingRSocket socket = new TestingRSocket(Function.identity());
          TestingRSocket failingSocket =
              new TestingRSocket(Function.identity()) {
                @Override
                public Mono<Payload> requestResponse(Payload payload) {
                  return Mono.error(new RuntimeException("You shouldn't be here"));
                }

                @Override
                public double availability() {
                  return 0.0;
                }
              };

          RSocketSupplier failing = succeedingFactory(failingSocket);
          RSocketSupplier succeeding = succeedingFactory(socket);
          List<RSocketSupplier> clients = Arrays.asList(failing, succeeding);

          testBalancer(clients);
        });
  }

  private void testBalancer(List<RSocketSupplier> factories) throws InterruptedException {
    Publisher<List<RSocketSupplier>> src =
        s -> {
          s.onNext(factories);
          s.onComplete();
        };

    LoadBalancedRSocketMono balancer = LoadBalancedRSocketMono.create(src);

    while (balancer.availability() == 0.0) {
      Thread.sleep(1);
    }

    Flux.range(0, 100).flatMap(i -> balancer).blockLast();
  }

  private void makeAcall(RSocket balancer) throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);

    balancer
        .requestResponse(PayloadImpl.EMPTY)
        .subscribe(
            new Subscriber<Payload>() {
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
                fail(t);
                latch.countDown();
              }

              @Override
              public void onComplete() {
                latch.countDown();
              }
            });

    latch.await();
  }

  private static RSocketSupplier succeedingFactory(RSocket socket) {
    RSocketSupplier mock = mock(RSocketSupplier.class);

    Mockito.when(mock.availability()).thenReturn(1.0);
    Mockito.when(mock.get()).thenReturn(Mono.just(socket));

    return mock;
  }

  private static RSocketSupplier failingClient(SocketAddress sa) {
    RSocketSupplier mock = mock(RSocketSupplier.class);

    Mockito.when(mock.availability()).thenReturn(0.0);
    Mockito.when(mock.get())
        .thenAnswer(
            a -> {
              fail("failing client");
              return null;
            });

    return mock;
  }
}
