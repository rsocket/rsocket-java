/*
 * Copyright 2015-2021 the original author or authors.
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
package io.rsocket.loadbalance;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.transport.ClientTransport;
import io.rsocket.util.EmptyPayload;
import io.rsocket.util.RSocketProxy;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.util.RaceTestUtils;
import reactor.util.context.Context;

public class LoadbalanceTest {

  @BeforeEach
  void setUp() {
    Hooks.onErrorDropped((__) -> {});
  }

  @AfterAll
  static void afterAll() {
    Hooks.resetOnErrorDropped();
  }

  @Test
  public void shouldDeliverAllTheRequestsWithRoundRobinStrategy() throws Exception {
    final AtomicInteger counter = new AtomicInteger();
    final ClientTransport mockTransport = Mockito.mock(ClientTransport.class);
    final RSocketConnector rSocketConnectorMock = Mockito.mock(RSocketConnector.class);

    Mockito.when(rSocketConnectorMock.connect(Mockito.any(ClientTransport.class)))
        .then(
            im ->
                Mono.just(
                    new TestRSocket(
                        new RSocket() {
                          @Override
                          public Mono<Void> fireAndForget(Payload payload) {
                            counter.incrementAndGet();
                            return Mono.empty();
                          }
                        })));

    for (int i = 0; i < 1000; i++) {
      final TestPublisher<List<LoadbalanceTarget>> source = TestPublisher.create();
      final RSocketPool rSocketPool =
          new RSocketPool(rSocketConnectorMock, source, new RoundRobinLoadbalanceStrategy());

      RaceTestUtils.race(
          () -> {
            for (int j = 0; j < 1000; j++) {
              Mono.defer(() -> rSocketPool.select().fireAndForget(EmptyPayload.INSTANCE))
                  .retry()
                  .subscribe();
            }
          },
          () -> {
            for (int j = 0; j < 100; j++) {
              source.next(Collections.emptyList());
              source.next(Collections.singletonList(LoadbalanceTarget.from("1", mockTransport)));
              source.next(
                  Arrays.asList(
                      LoadbalanceTarget.from("1", mockTransport),
                      LoadbalanceTarget.from("2", mockTransport)));
              source.next(Collections.singletonList(LoadbalanceTarget.from("1", mockTransport)));
              source.next(Collections.singletonList(LoadbalanceTarget.from("2", mockTransport)));
              source.next(Collections.emptyList());
              source.next(Collections.singletonList(LoadbalanceTarget.from("2", mockTransport)));
            }
          });

      Assertions.assertThat(counter.get()).isEqualTo(1000);
      counter.set(0);
    }
  }

  @Test
  public void shouldDeliverAllTheRequestsWithWeightedStrategy() throws InterruptedException {
    final AtomicInteger counter = new AtomicInteger();

    final ClientTransport mockTransport1 = Mockito.mock(ClientTransport.class);
    final ClientTransport mockTransport2 = Mockito.mock(ClientTransport.class);

    final LoadbalanceTarget target1 = LoadbalanceTarget.from("1", mockTransport1);
    final LoadbalanceTarget target2 = LoadbalanceTarget.from("2", mockTransport2);

    final WeightedRSocket weightedRSocket1 = new WeightedRSocket(counter);
    final WeightedRSocket weightedRSocket2 = new WeightedRSocket(counter);

    final RSocketConnector rSocketConnectorMock = Mockito.mock(RSocketConnector.class);
    Mockito.when(rSocketConnectorMock.connect(mockTransport1))
        .then(im -> Mono.just(new TestRSocket(weightedRSocket1)));
    Mockito.when(rSocketConnectorMock.connect(mockTransport2))
        .then(im -> Mono.just(new TestRSocket(weightedRSocket2)));

    for (int i = 0; i < 1000; i++) {
      final TestPublisher<List<LoadbalanceTarget>> source = TestPublisher.create();
      final RSocketPool rSocketPool =
          new RSocketPool(
              rSocketConnectorMock,
              source,
              WeightedLoadbalanceStrategy.builder()
                  .weightedStatsResolver(
                      rsocket -> {
                        if (rsocket instanceof TestRSocket) {
                          return (WeightedRSocket) ((TestRSocket) rsocket).source();
                        }
                        return ((PooledRSocket) rsocket).target() == target1
                            ? weightedRSocket1
                            : weightedRSocket2;
                      })
                  .build());

      RaceTestUtils.race(
          () -> {
            for (int j = 0; j < 1000; j++) {
              Mono.defer(() -> rSocketPool.select().fireAndForget(EmptyPayload.INSTANCE))
                  .retry()
                  .subscribe(aVoid -> {}, Throwable::printStackTrace);
            }
          },
          () -> {
            for (int j = 0; j < 100; j++) {
              source.next(Collections.emptyList());
              source.next(Collections.singletonList(target1));
              source.next(Arrays.asList(target1, target2)).next(Collections.singletonList(target1));
              source.next(Collections.singletonList(target2));
              source.next(Collections.emptyList());
              source.next(Collections.singletonList(target2));
            }
          });

      Assertions.assertThat(counter.get()).isEqualTo(1000);
      counter.set(0);
    }
  }

  @Test
  public void ensureRSocketIsCleanedFromThePoolIfSourceRSocketIsDisposed() {
    final AtomicInteger counter = new AtomicInteger();
    final ClientTransport mockTransport = Mockito.mock(ClientTransport.class);
    final RSocketConnector rSocketConnectorMock = Mockito.mock(RSocketConnector.class);

    final TestRSocket testRSocket =
        new TestRSocket(
            new RSocket() {
              @Override
              public Mono<Void> fireAndForget(Payload payload) {
                counter.incrementAndGet();
                return Mono.empty();
              }
            });

    Mockito.when(rSocketConnectorMock.connect(Mockito.any(ClientTransport.class)))
        .then(im -> Mono.delay(Duration.ofMillis(200)).map(__ -> testRSocket));

    final TestPublisher<List<LoadbalanceTarget>> source = TestPublisher.create();
    final RSocketPool rSocketPool =
        new RSocketPool(rSocketConnectorMock, source, new RoundRobinLoadbalanceStrategy());

    source.next(Collections.singletonList(LoadbalanceTarget.from("1", mockTransport)));

    StepVerifier.create(rSocketPool.select().fireAndForget(EmptyPayload.INSTANCE))
        .expectSubscription()
        .expectComplete()
        .verify(Duration.ofSeconds(2));

    testRSocket.dispose();

    Assertions.assertThatThrownBy(
            () ->
                rSocketPool
                    .select()
                    .fireAndForget(EmptyPayload.INSTANCE)
                    .block(Duration.ofSeconds(2)))
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessage("Timeout on blocking read for 2000000000 NANOSECONDS");

    Assertions.assertThat(counter.get()).isOne();
  }

  @Test
  public void ensureContextIsPropagatedCorrectlyForRequestChannel() {
    final AtomicInteger counter = new AtomicInteger();
    final ClientTransport mockTransport = Mockito.mock(ClientTransport.class);
    final RSocketConnector rSocketConnectorMock = Mockito.mock(RSocketConnector.class);

    Mockito.when(rSocketConnectorMock.connect(Mockito.any(ClientTransport.class)))
        .then(
            im ->
                Mono.delay(Duration.ofMillis(200))
                    .map(
                        __ ->
                            new TestRSocket(
                                new RSocket() {
                                  @Override
                                  public Flux<Payload> requestChannel(Publisher<Payload> source) {
                                    counter.incrementAndGet();
                                    return Flux.from(source);
                                  }
                                })));

    final TestPublisher<List<LoadbalanceTarget>> source = TestPublisher.create();
    final RSocketPool rSocketPool =
        new RSocketPool(rSocketConnectorMock, source, new RoundRobinLoadbalanceStrategy());

    // check that context is propagated when there is no rsocket
    StepVerifier.create(
            rSocketPool
                .select()
                .requestChannel(
                    Flux.deferContextual(
                        cv -> {
                          if (cv.hasKey("test") && cv.get("test").equals("test")) {
                            return Flux.just(EmptyPayload.INSTANCE);
                          } else {
                            return Flux.error(
                                new IllegalStateException("Expected context to be propagated"));
                          }
                        }))
                .contextWrite(Context.of("test", "test")))
        .expectSubscription()
        .then(
            () ->
                source.next(Collections.singletonList(LoadbalanceTarget.from("1", mockTransport))))
        .expectNextCount(1)
        .expectComplete()
        .verify(Duration.ofSeconds(2));

    source.next(Collections.singletonList(LoadbalanceTarget.from("2", mockTransport)));
    // check that context is propagated when there is an RSocket but it is unresolved
    StepVerifier.create(
            rSocketPool
                .select()
                .requestChannel(
                    Flux.deferContextual(
                        cv -> {
                          if (cv.hasKey("test") && cv.get("test").equals("test")) {
                            return Flux.just(EmptyPayload.INSTANCE);
                          } else {
                            return Flux.error(
                                new IllegalStateException("Expected context to be propagated"));
                          }
                        }))
                .contextWrite(Context.of("test", "test")))
        .expectSubscription()
        .expectNextCount(1)
        .expectComplete()
        .verify(Duration.ofSeconds(2));

    // check that context is propagated when there is an RSocket and it is resolved
    StepVerifier.create(
            rSocketPool
                .select()
                .requestChannel(
                    Flux.deferContextual(
                        cv -> {
                          if (cv.hasKey("test") && cv.get("test").equals("test")) {
                            return Flux.just(EmptyPayload.INSTANCE);
                          } else {
                            return Flux.error(
                                new IllegalStateException("Expected context to be propagated"));
                          }
                        }))
                .contextWrite(Context.of("test", "test")))
        .expectSubscription()
        .expectNextCount(1)
        .expectComplete()
        .verify(Duration.ofSeconds(2));

    Assertions.assertThat(counter.get()).isEqualTo(3);
  }

  static class TestRSocket extends RSocketProxy {

    final Sinks.Empty<Void> sink = Sinks.empty();

    public TestRSocket(RSocket rSocket) {
      super(rSocket);
    }

    @Override
    public Mono<Void> onClose() {
      return sink.asMono();
    }

    @Override
    public void dispose() {
      sink.tryEmitEmpty();
    }

    public RSocket source() {
      return source;
    }
  }

  private static class WeightedRSocket extends BaseWeightedStats implements RSocket {

    private final AtomicInteger counter;

    public WeightedRSocket(AtomicInteger counter) {
      this.counter = counter;
    }

    @Override
    public Mono<Void> fireAndForget(Payload payload) {
      final long startTime = startRequest();
      counter.incrementAndGet();
      return Mono.<Void>empty()
          .doFinally(
              (__) -> {
                final long stopTime = stopRequest(startTime);
                record(stopTime - startTime);
              });
    }
  }
}
