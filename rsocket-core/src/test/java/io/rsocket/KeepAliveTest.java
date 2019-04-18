/*
 * Copyright 2015-2019 the original author or authors.
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

import static io.rsocket.keepalive.KeepAliveHandler.DefaultKeepAliveHandler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.rsocket.exceptions.ConnectionErrorException;
import io.rsocket.frame.FrameHeaderFlyweight;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.KeepAliveFrameFlyweight;
import io.rsocket.test.util.TestDuplexConnection;
import io.rsocket.util.DefaultPayload;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class KeepAliveTest {
  private static final int KEEP_ALIVE_INTERVAL = 100;
  private static final int KEEP_ALIVE_TIMEOUT = 1000;

  static Stream<Supplier<TestData>> testData() {
    return Stream.of(
        requester(KEEP_ALIVE_INTERVAL, KEEP_ALIVE_TIMEOUT),
        responder(KEEP_ALIVE_INTERVAL, KEEP_ALIVE_TIMEOUT));
  }

  static Supplier<TestData> requester(int tickPeriod, int timeout) {
    return () -> {
      TestDuplexConnection connection = new TestDuplexConnection();
      Errors errors = new Errors();
      RSocketClient rSocket =
          new RSocketClient(
              ByteBufAllocator.DEFAULT,
              connection,
              DefaultPayload::create,
              errors,
              StreamIdSupplier.clientSupplier(),
              tickPeriod,
              timeout,
              new DefaultKeepAliveHandler());
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
              ByteBufAllocator.DEFAULT,
              connection,
              handler,
              DefaultPayload::create,
              errors,
              tickPeriod,
              timeout,
              new DefaultKeepAliveHandler());
      return new TestData(rSocket, errors, connection);
    };
  }

  @ParameterizedTest
  @MethodSource("testData")
  void rSocketNotDisposedOnPresentKeepAlives(Supplier<TestData> testDataSupplier) {
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
  void rSocketDisposedOnMissingKeepAlives(Supplier<TestData> testDataSupplier) {
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
  void clientRequesterSendsKeepAlives() {
    TestData testData = requester(100, 1000).get();
    TestDuplexConnection connection = testData.connection();

    StepVerifier.create(Flux.from(connection.getSentAsPublisher()).take(3))
        .expectNextMatches(this::keepAliveFrameWithRespondFlag)
        .expectNextMatches(this::keepAliveFrameWithRespondFlag)
        .expectNextMatches(this::keepAliveFrameWithRespondFlag)
        .expectComplete()
        .verify(Duration.ofSeconds(5));
  }

  @Test
  void serverResponderSendsKeepAlives() {
    TestData testData = responder(100, 1000).get();
    TestDuplexConnection connection = testData.connection();
    Mono.delay(Duration.ofMillis(100))
        .subscribe(
            l ->
                connection.addToReceivedBuffer(
                    KeepAliveFrameFlyweight.encode(
                        ByteBufAllocator.DEFAULT, true, 0, Unpooled.EMPTY_BUFFER)));

    StepVerifier.create(Flux.from(connection.getSentAsPublisher()).take(1))
        .expectNextMatches(this::keepAliveFrameWithoutRespondFlag)
        .expectComplete()
        .verify(Duration.ofSeconds(5));
  }

  private boolean keepAliveFrameWithRespondFlag(ByteBuf frame) {
    return FrameHeaderFlyweight.frameType(frame) == FrameType.KEEPALIVE
        && KeepAliveFrameFlyweight.respondFlag(frame);
  }

  private boolean keepAliveFrameWithoutRespondFlag(ByteBuf frame) {
    return FrameHeaderFlyweight.frameType(frame) == FrameType.KEEPALIVE
        && !KeepAliveFrameFlyweight.respondFlag(frame);
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
