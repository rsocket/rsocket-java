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
import static io.rsocket.keepalive.KeepAliveHandler.ResumableKeepAliveHandler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.rsocket.exceptions.ConnectionErrorException;
import io.rsocket.frame.FrameHeaderFlyweight;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.KeepAliveFrameFlyweight;
import io.rsocket.lease.RequesterLeaseHandler;
import io.rsocket.resume.InMemoryResumableFramesStore;
import io.rsocket.resume.ResumableDuplexConnection;
import io.rsocket.test.util.TestDuplexConnection;
import io.rsocket.util.DefaultPayload;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class KeepAliveTest {
  private static final int KEEP_ALIVE_INTERVAL = 100;
  private static final int KEEP_ALIVE_TIMEOUT = 1000;
  private static final int RESUMABLE_KEEP_ALIVE_TIMEOUT = 200;

  private RSocketState requesterState;
  private ResumableRSocketState resumableRequesterState;

  static RSocketState requester(int tickPeriod, int timeout) {
    TestDuplexConnection connection = new TestDuplexConnection();
    Errors errors = new Errors();
    RSocketRequester rSocket =
        new RSocketRequester(
            ByteBufAllocator.DEFAULT,
            connection,
            DefaultPayload::create,
            errors,
            StreamIdSupplier.clientSupplier(),
            tickPeriod,
            timeout,
            new DefaultKeepAliveHandler(connection),
            RequesterLeaseHandler.None);
    return new RSocketState(rSocket, errors, connection);
  }

  static ResumableRSocketState resumableRequester(int tickPeriod, int timeout) {
    TestDuplexConnection connection = new TestDuplexConnection();
    ResumableDuplexConnection resumableConnection =
        new ResumableDuplexConnection(
            "test",
            connection,
            new InMemoryResumableFramesStore("test", 10_000),
            Duration.ofSeconds(10),
            false);

    Errors errors = new Errors();
    RSocketRequester rSocket =
        new RSocketRequester(
            ByteBufAllocator.DEFAULT,
            resumableConnection,
            DefaultPayload::create,
            errors,
            StreamIdSupplier.clientSupplier(),
            tickPeriod,
            timeout,
            new ResumableKeepAliveHandler(resumableConnection),
            RequesterLeaseHandler.None);
    return new ResumableRSocketState(rSocket, errors, connection, resumableConnection);
  }

  @BeforeEach
  void setUp() {
    requesterState = requester(KEEP_ALIVE_INTERVAL, KEEP_ALIVE_TIMEOUT);
    resumableRequesterState = resumableRequester(KEEP_ALIVE_INTERVAL, RESUMABLE_KEEP_ALIVE_TIMEOUT);
  }

  @Test
  void rSocketNotDisposedOnPresentKeepAlives() {
    TestDuplexConnection connection = requesterState.connection();

    Flux.interval(Duration.ofMillis(100))
        .subscribe(
            n ->
                connection.addToReceivedBuffer(
                    KeepAliveFrameFlyweight.encode(
                        ByteBufAllocator.DEFAULT, true, 0, Unpooled.EMPTY_BUFFER)));

    Mono.delay(Duration.ofMillis(2000)).block();

    RSocket rSocket = requesterState.rSocket();
    List<Throwable> errors = requesterState.errors().errors();

    Assertions.assertThat(rSocket.isDisposed()).isFalse();
    Assertions.assertThat(errors).isEmpty();
  }

  @Test
  void noKeepAlivesSentAfterRSocketDispose() {
    requesterState.rSocket().dispose();
    StepVerifier.create(
            Flux.from(requesterState.connection().getSentAsPublisher())
                .take(Duration.ofMillis(500)))
        .expectComplete()
        .verify(Duration.ofSeconds(1));
  }

  @Test
  void rSocketDisposedOnMissingKeepAlives() {
    RSocket rSocket = requesterState.rSocket();

    Mono.delay(Duration.ofMillis(2000)).block();

    List<Throwable> errors = requesterState.errors().errors();
    Assertions.assertThat(rSocket.isDisposed()).isTrue();
    Assertions.assertThat(errors).hasSize(1);
    Throwable throwable = errors.get(0);
    Assertions.assertThat(throwable).isInstanceOf(ConnectionErrorException.class);
  }

  @Test
  void clientRequesterSendsKeepAlives() {
    RSocketState RSocketState = requester(100, 1000);
    TestDuplexConnection connection = RSocketState.connection();

    StepVerifier.create(Flux.from(connection.getSentAsPublisher()).take(3))
        .expectNextMatches(this::keepAliveFrameWithRespondFlag)
        .expectNextMatches(this::keepAliveFrameWithRespondFlag)
        .expectNextMatches(this::keepAliveFrameWithRespondFlag)
        .expectComplete()
        .verify(Duration.ofSeconds(5));
  }

  @Test
  void requesterRespondsToKeepAlives() {
    RSocketState RSocketState = requester(100_000, 100_000);
    TestDuplexConnection connection = RSocketState.connection();
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

  @Test
  void resumableRequesterNoKeepAlivesAfterDisconnect() {
    ResumableRSocketState rSocketState =
        resumableRequester(KEEP_ALIVE_INTERVAL, KEEP_ALIVE_TIMEOUT);
    TestDuplexConnection testConnection = rSocketState.connection();
    ResumableDuplexConnection resumableDuplexConnection = rSocketState.resumableDuplexConnection();

    resumableDuplexConnection.disconnect();

    StepVerifier.create(Flux.from(testConnection.getSentAsPublisher()).take(Duration.ofMillis(500)))
        .expectComplete()
        .verify(Duration.ofSeconds(5));
  }

  @Test
  void resumableRequesterKeepAlivesAfterReconnect() {
    ResumableRSocketState rSocketState =
        resumableRequester(KEEP_ALIVE_INTERVAL, KEEP_ALIVE_TIMEOUT);
    ResumableDuplexConnection resumableDuplexConnection = rSocketState.resumableDuplexConnection();
    resumableDuplexConnection.disconnect();
    TestDuplexConnection newTestConnection = new TestDuplexConnection();
    resumableDuplexConnection.reconnect(newTestConnection);
    resumableDuplexConnection.resume(0, 0, ignored -> Mono.empty());

    StepVerifier.create(Flux.from(newTestConnection.getSentAsPublisher()).take(1))
        .expectNextMatches(this::keepAliveFrame)
        .expectComplete()
        .verify(Duration.ofSeconds(5));
  }

  @Test
  void resumableRequesterNoKeepAlivesAfterDispose() {
    ResumableRSocketState rSocketState =
        resumableRequester(KEEP_ALIVE_INTERVAL, KEEP_ALIVE_TIMEOUT);
    rSocketState.rSocket().dispose();
    StepVerifier.create(
            Flux.from(rSocketState.connection().getSentAsPublisher()).take(Duration.ofMillis(500)))
        .expectComplete()
        .verify(Duration.ofSeconds(5));
  }

  @Test
  void resumableRSocketsNotDisposedOnMissingKeepAlives() {
    RSocket rSocket = resumableRequesterState.rSocket();
    List<Throwable> errors = resumableRequesterState.errors().errors();
    TestDuplexConnection connection = resumableRequesterState.connection();

    Mono.delay(Duration.ofMillis(500)).block();

    Assertions.assertThat(rSocket.isDisposed()).isFalse();
    Assertions.assertThat(errors).hasSize(0);
    Assertions.assertThat(connection.isDisposed()).isTrue();
  }

  private boolean keepAliveFrame(ByteBuf frame) {
    return FrameHeaderFlyweight.frameType(frame) == FrameType.KEEPALIVE;
  }

  private boolean keepAliveFrameWithRespondFlag(ByteBuf frame) {
    return keepAliveFrame(frame) && KeepAliveFrameFlyweight.respondFlag(frame);
  }

  private boolean keepAliveFrameWithoutRespondFlag(ByteBuf frame) {
    return keepAliveFrame(frame) && !KeepAliveFrameFlyweight.respondFlag(frame);
  }

  static class RSocketState {
    private final RSocket rSocket;
    private final Errors errors;
    private final TestDuplexConnection connection;

    public RSocketState(RSocket rSocket, Errors errors, TestDuplexConnection connection) {
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

  static class ResumableRSocketState {
    private final RSocket rSocket;
    private final Errors errors;
    private final TestDuplexConnection connection;
    private final ResumableDuplexConnection resumableDuplexConnection;

    public ResumableRSocketState(
        RSocket rSocket,
        Errors errors,
        TestDuplexConnection connection,
        ResumableDuplexConnection resumableDuplexConnection) {
      this.rSocket = rSocket;
      this.errors = errors;
      this.connection = connection;
      this.resumableDuplexConnection = resumableDuplexConnection;
    }

    public TestDuplexConnection connection() {
      return connection;
    }

    public ResumableDuplexConnection resumableDuplexConnection() {
      return resumableDuplexConnection;
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
