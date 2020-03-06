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
package io.rsocket.core;

import static org.junit.Assert.assertEquals;

import io.rsocket.RSocket;
import io.rsocket.test.util.TestClientTransport;
import io.rsocket.transport.ClientTransport;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

public class RSocketReconnectTest {

  private Queue<Retry.RetrySignal> retries = new ConcurrentLinkedQueue<>();

  @Test
  public void shouldBeASharedReconnectableInstanceOfRSocketMono() throws InterruptedException {
    TestClientTransport[] testClientTransport =
        new TestClientTransport[] {new TestClientTransport()};
    Mono<RSocket> rSocketMono =
        RSocketConnector.create()
            .reconnect(Retry.indefinitely())
            .connect(() -> testClientTransport[0]);

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
    ClientTransport transport = Mockito.mock(ClientTransport.class);
    Mockito.when(transport.connect())
        .thenThrow(UncheckedIOException.class)
        .thenThrow(UncheckedIOException.class)
        .thenThrow(UncheckedIOException.class)
        .thenThrow(UncheckedIOException.class)
        .thenReturn(new TestClientTransport().connect());
    Mono<RSocket> rSocketMono =
        RSocketConnector.create()
            .reconnect(
                Retry.backoff(4, Duration.ofMillis(100))
                    .maxBackoff(Duration.ofMillis(500))
                    .doAfterRetry(onRetry()))
            .connect(transport);

    RSocket rSocket1 = rSocketMono.block();
    RSocket rSocket2 = rSocketMono.block();

    Assertions.assertThat(rSocket1).isEqualTo(rSocket2);
    assertRetries(
        UncheckedIOException.class,
        UncheckedIOException.class,
        UncheckedIOException.class,
        UncheckedIOException.class);
  }

  @Test
  @SuppressWarnings({"rawtype", "unchecked"})
  public void shouldBeExaustedRetrieableConnectionSharedReconnectableInstanceOfRSocketMono() {
    ClientTransport transport = Mockito.mock(ClientTransport.class);
    Mockito.when(transport.connect())
        .thenThrow(UncheckedIOException.class)
        .thenThrow(UncheckedIOException.class)
        .thenThrow(UncheckedIOException.class)
        .thenThrow(UncheckedIOException.class)
        .thenThrow(UncheckedIOException.class)
        .thenReturn(new TestClientTransport().connect());
    Mono<RSocket> rSocketMono =
        RSocketConnector.create()
            .reconnect(
                Retry.backoff(4, Duration.ofMillis(100))
                    .maxBackoff(Duration.ofMillis(500))
                    .doAfterRetry(onRetry()))
            .connect(transport);

    Assertions.assertThatThrownBy(rSocketMono::block)
        .matches(Exceptions::isRetryExhausted)
        .hasCauseInstanceOf(UncheckedIOException.class);

    Assertions.assertThatThrownBy(rSocketMono::block)
        .matches(Exceptions::isRetryExhausted)
        .hasCauseInstanceOf(UncheckedIOException.class);

    assertRetries(
        UncheckedIOException.class,
        UncheckedIOException.class,
        UncheckedIOException.class,
        UncheckedIOException.class);
  }

  @Test
  public void shouldBeNotBeASharedReconnectableInstanceOfRSocketMono() {

    Mono<RSocket> rSocketMono = RSocketConnector.connectWith(new TestClientTransport());

    RSocket rSocket1 = rSocketMono.block();
    RSocket rSocket2 = rSocketMono.block();

    Assertions.assertThat(rSocket1).isNotEqualTo(rSocket2);
  }

  @SafeVarargs
  private final void assertRetries(Class<? extends Throwable>... exceptions) {
    assertEquals(exceptions.length, retries.size());
    int index = 0;
    for (Iterator<Retry.RetrySignal> it = retries.iterator(); it.hasNext(); ) {
      Retry.RetrySignal retryContext = it.next();
      assertEquals(index, retryContext.totalRetries());
      assertEquals(exceptions[index], retryContext.failure().getClass());
      index++;
    }
  }

  Consumer<Retry.RetrySignal> onRetry() {
    return context -> retries.add(context);
  }
}
