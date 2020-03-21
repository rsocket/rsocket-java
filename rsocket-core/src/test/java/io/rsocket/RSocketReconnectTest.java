/*
 * Copyright 2015-2020 the original author or authors.
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
package io.rsocket;

import static org.junit.Assert.assertEquals;

import io.rsocket.test.util.TestClientTransport;
import io.rsocket.transport.ClientTransport;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;
import reactor.retry.Retry;
import reactor.retry.RetryContext;
import reactor.retry.RetryExhaustedException;

public class RSocketReconnectTest {

  private Queue<RetryContext<?>> retries = new ConcurrentLinkedQueue<>();

  @Test
  public void shouldBeASharedReconnectableInstanceOfRSocketMono() {
    TestClientTransport[] testClientTransport =
        new TestClientTransport[] {new TestClientTransport()};
    Mono<RSocket> rSocketMono =
        RSocketFactory.connect()
            .singleSubscriberRequester()
            .reconnect()
            .transport(
                () -> {
                  return testClientTransport[0];
                })
            .start();

    RSocket rSocket1 = rSocketMono.block();
    RSocket rSocket2 = rSocketMono.block();

    Assertions.assertThat(rSocket1).isEqualTo(rSocket2);

    testClientTransport[0].testConnection().dispose();
    testClientTransport[0] = new TestClientTransport();

    RSocket rSocket3 = rSocketMono.block();
    RSocket rSocket4 = rSocketMono.block();

    Assertions.assertThat(rSocket3).isEqualTo(rSocket4).isNotEqualTo(rSocket2);
  }

  @Test
  @SuppressWarnings({"rawtype", "unchecked"})
  public void shouldBeRetrieableConnectionSharedReconnectableInstanceOfRSocketMono() {
    Supplier<ClientTransport> mockTransportSupplier = Mockito.mock(Supplier.class);
    Mockito.when(mockTransportSupplier.get())
        .thenThrow(UncheckedIOException.class)
        .thenThrow(UncheckedIOException.class)
        .thenThrow(UncheckedIOException.class)
        .thenThrow(UncheckedIOException.class)
        .thenReturn(new TestClientTransport());
    Mono<RSocket> rSocketMono =
        RSocketFactory.connect()
            .singleSubscriberRequester()
            .reconnect(
                Retry.any()
                    .exponentialBackoff(Duration.ofMillis(100), Duration.ofMillis(500))
                    .retryMax(4)
                    .doOnRetry(onRetry()))
            .transport(mockTransportSupplier)
            .start();

    RSocket rSocket1 = rSocketMono.block();
    RSocket rSocket2 = rSocketMono.block();

    Assertions.assertThat(rSocket1).isEqualTo(rSocket2);
    assertRetries(
        UncheckedIOException.class,
        UncheckedIOException.class,
        UncheckedIOException.class,
        UncheckedIOException.class);
    RetryTestUtils.assertDelays(retries, 100L, 200L, 400L, 500L);
  }

  @Test
  @SuppressWarnings({"rawtype", "unchecked"})
  public void shouldBeExaustedRetrieableConnectionSharedReconnectableInstanceOfRSocketMono() {
    Supplier<ClientTransport> mockTransportSupplier = Mockito.mock(Supplier.class);
    Mockito.when(mockTransportSupplier.get())
        .thenThrow(UncheckedIOException.class)
        .thenThrow(UncheckedIOException.class)
        .thenThrow(UncheckedIOException.class)
        .thenThrow(UncheckedIOException.class)
        .thenThrow(UncheckedIOException.class)
        .thenReturn(new TestClientTransport());
    Mono<RSocket> rSocketMono =
        RSocketFactory.connect()
            .singleSubscriberRequester()
            .reconnect(
                Retry.any()
                    .exponentialBackoff(Duration.ofMillis(100), Duration.ofMillis(500))
                    .retryMax(4)
                    .doOnRetry(onRetry()))
            .transport(mockTransportSupplier)
            .start();

    Assertions.assertThatThrownBy(rSocketMono::block)
        .isInstanceOf(RetryExhaustedException.class)
        .hasCauseInstanceOf(UncheckedIOException.class);

    Assertions.assertThatThrownBy(rSocketMono::block)
        .isInstanceOf(RetryExhaustedException.class)
        .hasCauseInstanceOf(UncheckedIOException.class);

    assertRetries(
        UncheckedIOException.class,
        UncheckedIOException.class,
        UncheckedIOException.class,
        UncheckedIOException.class);
    RetryTestUtils.assertDelays(retries, 100L, 200L, 400L, 500L);
  }

  @Test
  public void shouldBeNotBeASharedReconnectableInstanceOfRSocketMono() {

    Mono<RSocket> rSocketMono =
        RSocketFactory.connect()
            .singleSubscriberRequester()
            .transport(new TestClientTransport())
            .start();

    RSocket rSocket1 = rSocketMono.block();
    RSocket rSocket2 = rSocketMono.block();

    Assertions.assertThat(rSocket1).isNotEqualTo(rSocket2);
  }

  @SafeVarargs
  private final void assertRetries(Class<? extends Throwable>... exceptions) {
    assertEquals(exceptions.length, retries.size());
    int index = 0;
    for (Iterator<RetryContext<?>> it = retries.iterator(); it.hasNext(); ) {
      RetryContext<?> retryContext = it.next();
      assertEquals(index + 1, retryContext.iteration());
      assertEquals(exceptions[index], retryContext.exception().getClass());
      index++;
    }
  }

  Consumer<? super RetryContext<?>> onRetry() {
    return context -> retries.add(context);
  }
}
