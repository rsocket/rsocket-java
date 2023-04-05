package io.rsocket.loadbalance;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RaceTestConstants;
import io.rsocket.core.RSocketConnector;
import io.rsocket.transport.ClientTransport;
import io.rsocket.util.Clock;
import io.rsocket.util.EmptyPayload;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.assertj.core.api.Assertions;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.publisher.TestPublisher;

public class WeightedLoadbalanceStrategyTest {

  @BeforeEach
  void setUp() {
    Hooks.onErrorDropped((__) -> {});
  }

  @AfterAll
  static void afterAll() {
    Hooks.resetOnErrorDropped();
  }

  @Test
  public void allRequestsShouldGoToTheSocketWithHigherWeight() {
    final AtomicInteger counter1 = new AtomicInteger();
    final AtomicInteger counter2 = new AtomicInteger();
    final ClientTransport mockTransport = Mockito.mock(ClientTransport.class);
    final RSocketConnector rSocketConnectorMock = Mockito.mock(RSocketConnector.class);
    final WeightedTestRSocket rSocket1 =
        new WeightedTestRSocket(
            new RSocket() {
              @Override
              public Mono<Void> fireAndForget(Payload payload) {
                counter1.incrementAndGet();
                return Mono.empty();
              }
            });
    final WeightedTestRSocket rSocket2 =
        new WeightedTestRSocket(
            new RSocket() {
              @Override
              public Mono<Void> fireAndForget(Payload payload) {
                counter2.incrementAndGet();
                return Mono.empty();
              }
            });
    Mockito.when(rSocketConnectorMock.connect(Mockito.any(ClientTransport.class)))
        .then(im -> Mono.just(rSocket1))
        .then(im -> Mono.just(rSocket2));

    final TestPublisher<List<LoadbalanceTarget>> source = TestPublisher.create();
    final RSocketPool rSocketPool =
        new RSocketPool(
            rSocketConnectorMock,
            source,
            WeightedLoadbalanceStrategy.builder()
                .weightedStatsResolver(r -> r instanceof WeightedStats ? (WeightedStats) r : null)
                .build());

    for (int j = 0; j < RaceTestConstants.REPEATS; j++) {
      rSocketPool.select().fireAndForget(EmptyPayload.INSTANCE).subscribe();
    }

    source.next(
        Arrays.asList(
            LoadbalanceTarget.from("1", mockTransport),
            LoadbalanceTarget.from("2", mockTransport)));

    Assertions.assertThat(counter1.get())
        .describedAs("c1=" + counter1.get() + " c2=" + counter2.get())
        .isCloseTo(
            RaceTestConstants.REPEATS, Offset.offset(Math.round(RaceTestConstants.REPEATS * 0.1f)));
    Assertions.assertThat(counter2.get())
        .describedAs("c1=" + counter1.get() + " c2=" + counter2.get())
        .isCloseTo(0, Offset.offset(Math.round(RaceTestConstants.REPEATS * 0.1f)));
  }

  @Test
  public void shouldDeliverValuesToTheSocketWithTheHighestCalculatedWeight() {
    final AtomicInteger counter1 = new AtomicInteger();
    final AtomicInteger counter2 = new AtomicInteger();
    final ClientTransport mockTransport1 = Mockito.mock(ClientTransport.class);
    final ClientTransport mockTransport2 = Mockito.mock(ClientTransport.class);
    final RSocketConnector rSocketConnectorMock = Mockito.mock(RSocketConnector.class);
    final WeightedTestRSocket rSocket1 =
        new WeightedTestRSocket(
            new RSocket() {
              @Override
              public Mono<Void> fireAndForget(Payload payload) {
                counter1.incrementAndGet();
                return Mono.empty();
              }
            });
    final WeightedTestRSocket rSocket2 =
        new WeightedTestRSocket(
            new RSocket() {
              @Override
              public Mono<Void> fireAndForget(Payload payload) {
                counter1.incrementAndGet();
                return Mono.empty();
              }
            });
    final WeightedTestRSocket rSocket3 =
        new WeightedTestRSocket(
            new RSocket() {
              @Override
              public Mono<Void> fireAndForget(Payload payload) {
                counter2.incrementAndGet();
                return Mono.empty();
              }
            });

    Mockito.when(rSocketConnectorMock.connect(Mockito.any(ClientTransport.class)))
        .then(im -> Mono.just(rSocket1))
        .then(im -> Mono.just(rSocket2))
        .then(im -> Mono.just(rSocket3));

    final TestPublisher<List<LoadbalanceTarget>> source = TestPublisher.create();
    final RSocketPool rSocketPool =
        new RSocketPool(
            rSocketConnectorMock,
            source,
            WeightedLoadbalanceStrategy.builder()
                .weightedStatsResolver(r -> r instanceof WeightedStats ? (WeightedStats) r : null)
                .build());

    for (int j = 0; j < RaceTestConstants.REPEATS; j++) {
      rSocketPool.select().fireAndForget(EmptyPayload.INSTANCE).subscribe();
    }

    source.next(Collections.singletonList(LoadbalanceTarget.from("1", mockTransport1)));

    Assertions.assertThat(counter1.get()).isCloseTo(RaceTestConstants.REPEATS, Offset.offset(1));

    source.next(Collections.emptyList());

    for (int j = 0; j < RaceTestConstants.REPEATS; j++) {
      rSocketPool.select().fireAndForget(EmptyPayload.INSTANCE).subscribe();
    }

    rSocket1.updateAvailability(0.0);

    source.next(Collections.singletonList(LoadbalanceTarget.from("1", mockTransport1)));

    Assertions.assertThat(counter1.get())
        .isCloseTo(RaceTestConstants.REPEATS * 2, Offset.offset(1));

    source.next(
        Arrays.asList(
            LoadbalanceTarget.from("1", mockTransport1),
            LoadbalanceTarget.from("2", mockTransport2)));

    for (int j = 0; j < RaceTestConstants.REPEATS; j++) {
      final RSocket rSocket = rSocketPool.select();
      rSocket.fireAndForget(EmptyPayload.INSTANCE).subscribe();
    }

    Assertions.assertThat(counter1.get())
        .isCloseTo(
            RaceTestConstants.REPEATS * 3,
            Offset.offset(Math.round(RaceTestConstants.REPEATS * 3 * 0.1f)));
    Assertions.assertThat(counter2.get())
        .isCloseTo(0, Offset.offset(Math.round(RaceTestConstants.REPEATS * 3 * 0.1f)));

    rSocket2.updateAvailability(0.0);

    source.next(Collections.singletonList(LoadbalanceTarget.from("2", mockTransport1)));

    for (int j = 0; j < RaceTestConstants.REPEATS; j++) {
      rSocketPool.select().fireAndForget(EmptyPayload.INSTANCE).subscribe();
    }

    Assertions.assertThat(counter1.get())
        .isCloseTo(
            RaceTestConstants.REPEATS * 3,
            Offset.offset(Math.round(RaceTestConstants.REPEATS * 4 * 0.1f)));
    Assertions.assertThat(counter2.get())
        .isCloseTo(
            RaceTestConstants.REPEATS,
            Offset.offset(Math.round(RaceTestConstants.REPEATS * 4 * 0.1f)));

    source.next(
        Arrays.asList(
            LoadbalanceTarget.from("1", mockTransport1),
            LoadbalanceTarget.from("2", mockTransport2)));

    for (int j = 0; j < RaceTestConstants.REPEATS; j++) {
      final RSocket rSocket = rSocketPool.select();
      rSocket.fireAndForget(EmptyPayload.INSTANCE).subscribe();
    }

    Assertions.assertThat(counter1.get())
        .isCloseTo(
            RaceTestConstants.REPEATS * 3,
            Offset.offset(Math.round(RaceTestConstants.REPEATS * 5 * 0.1f)));
    Assertions.assertThat(counter2.get())
        .isCloseTo(
            RaceTestConstants.REPEATS * 2,
            Offset.offset(Math.round(RaceTestConstants.REPEATS * 5 * 0.1f)));
  }

  static class WeightedTestRSocket extends BaseWeightedStats implements RSocket {

    final Sinks.Empty<Void> sink = Sinks.empty();

    final RSocket rSocket;

    public WeightedTestRSocket(RSocket rSocket) {
      this.rSocket = rSocket;
    }

    @Override
    public Mono<Void> fireAndForget(Payload payload) {
      startRequest();
      final long startTime = Clock.now();
      return this.rSocket
          .fireAndForget(payload)
          .doFinally(
              __ -> {
                stopRequest(startTime);
                record(Clock.now() - startTime);
                updateAvailability(1.0);
              });
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
      return rSocket;
    }
  }
}
