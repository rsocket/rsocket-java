/*
 * Copyright 2015-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import io.rsocket.client.filter.RSocketSupplier;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class LoadBalancedRSocketMonoTest {

  @Test(timeout = 10_000L)
  public void testNeverSelectFailingFactories() throws InterruptedException {
    TestingRSocket socket = new TestingRSocket(Function.identity());
    RSocketSupplier failing = failingClient();
    RSocketSupplier succeeding = succeedingFactory(socket);
    List<RSocketSupplier> factories = Arrays.asList(failing, succeeding);

    testBalancer(factories);
  }

  @Test(timeout = 10_000L)
  public void testNeverSelectFailingSocket() throws InterruptedException {
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
  }

  @Test(timeout = 10_000L)
  @Ignore
  public void testRefreshesSocketsOnSelectBeforeReturningFailedAfterNewFactoriesDelivered() {
    TestingRSocket socket = new TestingRSocket(Function.identity());

    CompletableFuture<RSocketSupplier> laterSupplier = new CompletableFuture<>();
    Flux<List<RSocketSupplier>> factories =
        Flux.create(
            s -> {
              s.next(Collections.emptyList());

              laterSupplier.handle(
                  (RSocketSupplier result, Throwable t) -> {
                    s.next(Collections.singletonList(result));
                    return null;
                  });
            });

    LoadBalancedRSocketMono balancer = LoadBalancedRSocketMono.create(factories);

    Assert.assertEquals(0.0, balancer.availability(), 0);

    laterSupplier.complete(succeedingFactory(socket));
    balancer.rSocketMono.block();

    Assert.assertEquals(1.0, balancer.availability(), 0);
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

  private static RSocketSupplier succeedingFactory(RSocket socket) {
    RSocketSupplier mock = Mockito.mock(RSocketSupplier.class);

    Mockito.when(mock.availability()).thenReturn(1.0);
    Mockito.when(mock.get()).thenReturn(Mono.just(socket));
    Mockito.when(mock.onClose()).thenReturn(Mono.never());

    return mock;
  }

  private static RSocketSupplier failingClient() {
    RSocketSupplier mock = Mockito.mock(RSocketSupplier.class);

    Mockito.when(mock.availability()).thenReturn(0.0);
    Mockito.when(mock.get())
        .thenAnswer(
            a -> {
              Assert.fail();
              return null;
            });

    return mock;
  }
}
