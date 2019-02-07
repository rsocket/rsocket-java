package io.rsocket;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.rsocket.exceptions.ConnectionErrorException;
import io.rsocket.frame.FrameHeaderFlyweight;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.KeepAliveFrameFlyweight;
import io.rsocket.test.util.TestDuplexConnection;
import io.rsocket.util.DefaultPayload;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class KeepAliveTest {
  private static final int CLIENT_REQUESTER_TICK_PERIOD = 100;
  private static final int CLIENT_REQUESTER_TIMEOUT = 700;
  private static final int CLIENT_REQUESTER_MISSED_ACKS = 3;
  private static final int SERVER_RESPONDER_TICK_PERIOD = 100;
  private static final int SERVER_RESPONDER_TIMEOUT = 1000;

  static Stream<Supplier<TestData>> testData() {
    return Stream.of(
        requester(
            CLIENT_REQUESTER_TICK_PERIOD, CLIENT_REQUESTER_TIMEOUT, CLIENT_REQUESTER_MISSED_ACKS),
        responder(SERVER_RESPONDER_TICK_PERIOD, SERVER_RESPONDER_TIMEOUT));
  }

  static Supplier<TestData> requester(int tickPeriod, int timeout, int missedAcks) {
    return () -> {
      TestDuplexConnection connection = new TestDuplexConnection();
      Errors errors = new Errors();
      RSocketClient rSocket =
          new RSocketClient(
              connection,
              DefaultPayload::create,
              errors,
              StreamIdSupplier.clientSupplier(),
              Duration.ofMillis(tickPeriod),
              Duration.ofMillis(timeout),
              missedAcks);
      return new TestData(rSocket, errors, connection);
    };
  }

  static Supplier<TestData> responder(int tickPeriod, int timeout) {
    return () -> {
      TestDuplexConnection connection = new TestDuplexConnection();
      AbstractRSocket handler = new AbstractRSocket() {};
      Errors errors = new Errors();
      RSocketServer rSocket =
          new RSocketServer(
              connection, handler, DefaultPayload::create, errors, tickPeriod, timeout);
      return new TestData(rSocket, errors, connection);
    };
  }

  @ParameterizedTest
  @MethodSource("testData")
  void keepAlives(Supplier<TestData> testDataSupplier) {
    TestData testData = testDataSupplier.get();
    TestDuplexConnection connection = testData.connection();

    Flux.interval(Duration.ofMillis(100))
        .subscribe(
            n ->
                connection.addToReceivedBuffer(
                    KeepAliveFrameFlyweight.encode(
                        ByteBufAllocator.DEFAULT, true, 0, Unpooled.EMPTY_BUFFER)));

    Mono.delay(Duration.ofMillis(1500)).block();

    RSocket rSocket = testData.rSocket();
    List<Throwable> errors = testData.errors().errors();

    Assertions.assertThat(rSocket.isDisposed()).isFalse();
    Assertions.assertThat(errors).isEmpty();
  }

  @ParameterizedTest
  @MethodSource("testData")
  void keepAlivesMissing(Supplier<TestData> testDataSupplier) {
    TestData testData = testDataSupplier.get();
    RSocket rSocket = testData.rSocket();

    Mono.delay(Duration.ofMillis(1500)).block();

    List<Throwable> errors = testData.errors().errors();
    Assertions.assertThat(rSocket.isDisposed()).isTrue();
    Assertions.assertThat(errors).hasSize(1);
    Throwable throwable = errors.get(0);
    Assertions.assertThat(throwable).isInstanceOf(ConnectionErrorException.class);
  }

  @Test
  void clientRequesterRespondsToKeepAlives() {
    TestData testData = requester(100, 700, 3).get();
    TestDuplexConnection connection = testData.connection();

    Mono.delay(Duration.ofMillis(100))
        .subscribe(
            l ->
                connection.addToReceivedBuffer(
                    KeepAliveFrameFlyweight.encode(
                        ByteBufAllocator.DEFAULT, true, 0, Unpooled.EMPTY_BUFFER)));

    Mono<Void> keepAliveResponse =
        Flux.from(connection.getSentAsPublisher())
            .filter(
                f ->
                    FrameHeaderFlyweight.frameType(f) == FrameType.KEEPALIVE
                        && !KeepAliveFrameFlyweight.respondFlag(f))
            .next()
            .then();

    StepVerifier.create(keepAliveResponse).expectComplete().verify(Duration.ofSeconds(5));
  }

  static class TestData {
    private final RSocket rSocket;
    private final Errors errors;
    private final TestDuplexConnection connection;

    public TestData(RSocket rSocket, Errors errors, TestDuplexConnection connection) {
      this.rSocket = rSocket;
      this.errors = errors;
      this.connection = connection;
    }

    public TestDuplexConnection connection() {
      return connection;
    }

    public RSocket rSocket() {
      return rSocket;
    }

    public Errors errors() {
      return errors;
    }
  }

  static class Errors implements Consumer<Throwable> {
    private final List<Throwable> errors = new ArrayList<>();

    @Override
    public void accept(Throwable throwable) {
      errors.add(throwable);
    }

    public List<Throwable> errors() {
      return new ArrayList<>(errors);
    }
  }
}
