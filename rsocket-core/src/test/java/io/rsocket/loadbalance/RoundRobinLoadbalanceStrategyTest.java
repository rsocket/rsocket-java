package io.rsocket.loadbalance;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.transport.ClientTransport;
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
import reactor.test.publisher.TestPublisher;

public class RoundRobinLoadbalanceStrategyTest {

  @BeforeEach
  void setUp() {
    Hooks.onErrorDropped((__) -> {});
  }

  @AfterAll
  static void afterAll() {
    Hooks.resetOnErrorDropped();
  }

  @Test
  public void shouldDeliverValuesProportionally() {
    final AtomicInteger counter1 = new AtomicInteger();
    final AtomicInteger counter2 = new AtomicInteger();
    final ClientTransport mockTransport = Mockito.mock(ClientTransport.class);
    final RSocketConnector rSocketConnectorMock = Mockito.mock(RSocketConnector.class);

    Mockito.when(rSocketConnectorMock.connect(Mockito.any(ClientTransport.class)))
        .then(
            im ->
                Mono.just(
                    new LoadbalanceTest.TestRSocket(
                        new RSocket() {
                          @Override
                          public Mono<Void> fireAndForget(Payload payload) {
                            counter1.incrementAndGet();
                            return Mono.empty();
                          }
                        })))
        .then(
            im ->
                Mono.just(
                    new LoadbalanceTest.TestRSocket(
                        new RSocket() {
                          @Override
                          public Mono<Void> fireAndForget(Payload payload) {
                            counter2.incrementAndGet();
                            return Mono.empty();
                          }
                        })));

    final TestPublisher<List<LoadbalanceTarget>> source = TestPublisher.create();
    final RSocketPool rSocketPool =
        new RSocketPool(rSocketConnectorMock, source, new RoundRobinLoadbalanceStrategy());

    for (int j = 0; j < 1000; j++) {
      rSocketPool.select().fireAndForget(EmptyPayload.INSTANCE).subscribe();
    }

    source.next(
        Arrays.asList(
            LoadbalanceTarget.from("1", mockTransport),
            LoadbalanceTarget.from("2", mockTransport)));

    Assertions.assertThat(counter1.get()).isCloseTo(500, Offset.offset(1));
    Assertions.assertThat(counter2.get()).isCloseTo(500, Offset.offset(1));
  }

  @Test
  public void shouldDeliverValuesToNewlyConnectedSockets() {
    final AtomicInteger counter1 = new AtomicInteger();
    final AtomicInteger counter2 = new AtomicInteger();
    final ClientTransport mockTransport1 = Mockito.mock(ClientTransport.class);
    final ClientTransport mockTransport2 = Mockito.mock(ClientTransport.class);
    final RSocketConnector rSocketConnectorMock = Mockito.mock(RSocketConnector.class);

    Mockito.when(rSocketConnectorMock.connect(Mockito.any(ClientTransport.class)))
        .then(
            im ->
                Mono.just(
                    new LoadbalanceTest.TestRSocket(
                        new RSocket() {
                          @Override
                          public Mono<Void> fireAndForget(Payload payload) {
                            if (im.getArgument(0) == mockTransport1) {
                              counter1.incrementAndGet();
                            } else {
                              counter2.incrementAndGet();
                            }
                            return Mono.empty();
                          }
                        })));

    final TestPublisher<List<LoadbalanceTarget>> source = TestPublisher.create();
    final RSocketPool rSocketPool =
        new RSocketPool(rSocketConnectorMock, source, new RoundRobinLoadbalanceStrategy());

    for (int j = 0; j < 1000; j++) {
      rSocketPool.select().fireAndForget(EmptyPayload.INSTANCE).subscribe();
    }

    source.next(Collections.singletonList(LoadbalanceTarget.from("1", mockTransport1)));

    Assertions.assertThat(counter1.get()).isCloseTo(1000, Offset.offset(1));

    source.next(Collections.emptyList());

    for (int j = 0; j < 1000; j++) {
      rSocketPool.select().fireAndForget(EmptyPayload.INSTANCE).subscribe();
    }

    source.next(Collections.singletonList(LoadbalanceTarget.from("1", mockTransport1)));

    Assertions.assertThat(counter1.get()).isCloseTo(2000, Offset.offset(1));

    source.next(
        Arrays.asList(
            LoadbalanceTarget.from("1", mockTransport1),
            LoadbalanceTarget.from("2", mockTransport2)));

    for (int j = 0; j < 1000; j++) {
      rSocketPool.select().fireAndForget(EmptyPayload.INSTANCE).subscribe();
    }

    Assertions.assertThat(counter1.get()).isCloseTo(2500, Offset.offset(1));
    Assertions.assertThat(counter2.get()).isCloseTo(500, Offset.offset(1));

    source.next(Collections.singletonList(LoadbalanceTarget.from("2", mockTransport1)));

    for (int j = 0; j < 1000; j++) {
      rSocketPool.select().fireAndForget(EmptyPayload.INSTANCE).subscribe();
    }

    Assertions.assertThat(counter1.get()).isCloseTo(2500, Offset.offset(1));
    Assertions.assertThat(counter2.get()).isCloseTo(1500, Offset.offset(1));

    source.next(
        Arrays.asList(
            LoadbalanceTarget.from("1", mockTransport1),
            LoadbalanceTarget.from("2", mockTransport2)));

    for (int j = 0; j < 1000; j++) {
      rSocketPool.select().fireAndForget(EmptyPayload.INSTANCE).subscribe();
    }

    Assertions.assertThat(counter1.get()).isCloseTo(3000, Offset.offset(1));
    Assertions.assertThat(counter2.get()).isCloseTo(2000, Offset.offset(1));
  }
}
