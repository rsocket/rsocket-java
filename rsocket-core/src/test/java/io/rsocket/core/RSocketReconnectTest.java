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

import static org.assertj.core.api.Assertions.assertThat;

import io.rsocket.FrameAssert;
import io.rsocket.RSocket;
import io.rsocket.frame.FrameType;
import io.rsocket.test.util.TestClientTransport;
import io.rsocket.test.util.TestDuplexConnection;
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

    FrameAssert.assertThat(testClientTransport[0].testConnection().awaitFrame())
        .typeOf(FrameType.SETUP)
        .hasStreamIdZero()
        .hasNoLeaks();

    assertThat(rSocket1).isEqualTo(rSocket2);

    testClientTransport[0].testConnection().dispose();
    rSocket1.onClose().block(Duration.ofSeconds(1));
    testClientTransport[0].alloc().assertHasNoLeaks();
    testClientTransport[0] = new TestClientTransport();

    RSocket rSocket3 = rSocketMono.block();
    RSocket rSocket4 = rSocketMono.block();

    FrameAssert.assertThat(testClientTransport[0].testConnection().awaitFrame())
        .typeOf(FrameType.SETUP)
        .hasStreamIdZero()
        .hasNoLeaks();

    assertThat(rSocket3).isEqualTo(rSocket4).isNotEqualTo(rSocket2);

    testClientTransport[0].testConnection().dispose();
    rSocket3.onClose().block(Duration.ofSeconds(1));
    testClientTransport[0].alloc().assertHasNoLeaks();
  }

  @Test
  @SuppressWarnings({"rawtype"})
  public void shouldBeRetrieableConnectionSharedReconnectableInstanceOfRSocketMono() {
    ClientTransport transport = Mockito.mock(ClientTransport.class);
    TestClientTransport transport1 = new TestClientTransport();
    Mockito.when(transport.connect())
        .thenThrow(UncheckedIOException.class)
        .thenThrow(UncheckedIOException.class)
        .thenThrow(UncheckedIOException.class)
        .thenThrow(UncheckedIOException.class)
        .thenReturn(transport1.connect());
    Mono<RSocket> rSocketMono =
        RSocketConnector.create()
            .reconnect(
                Retry.backoff(4, Duration.ofMillis(100))
                    .maxBackoff(Duration.ofMillis(500))
                    .doAfterRetry(onRetry()))
            .connect(transport);

    RSocket rSocket1 = rSocketMono.block();
    RSocket rSocket2 = rSocketMono.block();

    assertThat(rSocket1).isEqualTo(rSocket2);
    assertRetries(
        UncheckedIOException.class,
        UncheckedIOException.class,
        UncheckedIOException.class,
        UncheckedIOException.class);

    FrameAssert.assertThat(transport1.testConnection().awaitFrame())
        .typeOf(FrameType.SETUP)
        .hasStreamIdZero()
        .hasNoLeaks();

    transport1.testConnection().dispose();
    rSocket1.onClose().block(Duration.ofSeconds(1));
    transport1.alloc().assertHasNoLeaks();
  }

  @Test
  @SuppressWarnings({"rawtype"})
  public void shouldBeExaustedRetrieableConnectionSharedReconnectableInstanceOfRSocketMono() {
    ClientTransport transport = Mockito.mock(ClientTransport.class);
    TestClientTransport transport1 = new TestClientTransport();
    Mockito.when(transport.connect())
        .thenThrow(UncheckedIOException.class)
        .thenThrow(UncheckedIOException.class)
        .thenThrow(UncheckedIOException.class)
        .thenThrow(UncheckedIOException.class)
        .thenThrow(UncheckedIOException.class)
        .thenReturn(transport1.connect());
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

    transport1.alloc().assertHasNoLeaks();
  }

  @Test
  public void shouldBeNotBeASharedReconnectableInstanceOfRSocketMono() {
    TestClientTransport transport = new TestClientTransport();
    Mono<RSocket> rSocketMono = RSocketConnector.connectWith(transport);

    RSocket rSocket1 = rSocketMono.block();
    TestDuplexConnection connection1 = transport.testConnection();

    FrameAssert.assertThat(connection1.awaitFrame())
        .typeOf(FrameType.SETUP)
        .hasStreamIdZero()
        .hasNoLeaks();

    RSocket rSocket2 = rSocketMono.block();
    TestDuplexConnection connection2 = transport.testConnection();

    assertThat(rSocket1).isNotEqualTo(rSocket2);

    FrameAssert.assertThat(connection2.awaitFrame())
        .typeOf(FrameType.SETUP)
        .hasStreamIdZero()
        .hasNoLeaks();

    connection1.dispose();
    connection2.dispose();
    rSocket1.onClose().block(Duration.ofSeconds(1));
    rSocket2.onClose().block(Duration.ofSeconds(1));
    transport.alloc().assertHasNoLeaks();
  }

  @SafeVarargs
  private final void assertRetries(Class<? extends Throwable>... exceptions) {
    assertThat(retries.size()).isEqualTo(exceptions.length);
    int index = 0;
    for (Iterator<Retry.RetrySignal> it = retries.iterator(); it.hasNext(); ) {
      Retry.RetrySignal retryContext = it.next();
      assertThat(retryContext.totalRetries()).isEqualTo(index);
      assertThat(retryContext.failure().getClass()).isEqualTo(exceptions[index]);
      index++;
    }
  }

  Consumer<Retry.RetrySignal> onRetry() {
    return context -> retries.add(context);
  }
}
