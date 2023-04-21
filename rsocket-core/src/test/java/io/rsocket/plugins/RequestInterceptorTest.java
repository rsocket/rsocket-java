package io.rsocket.plugins;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Closeable;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.buffer.LeaksTrackingByteBufAllocator;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.frame.FrameType;
import io.rsocket.transport.local.LocalClientTransport;
import io.rsocket.transport.local.LocalServerTransport;
import io.rsocket.util.DefaultPayload;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.annotation.Nullable;

public class RequestInterceptorTest {

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void interceptorShouldBeInstalledProperlyOnTheClientRequesterSide(boolean errorOutcome) {
    final LeaksTrackingByteBufAllocator byteBufAllocator =
        LeaksTrackingByteBufAllocator.instrument(
            ByteBufAllocator.DEFAULT, Duration.ofSeconds(1), "test");
    final Closeable closeable =
        RSocketServer.create(
                SocketAcceptor.with(
                    new RSocket() {
                      @Override
                      public Mono<Void> fireAndForget(Payload payload) {
                        return Mono.empty();
                      }

                      @Override
                      public Mono<Payload> requestResponse(Payload payload) {
                        return errorOutcome
                            ? Mono.error(new RuntimeException("test"))
                            : Mono.just(payload);
                      }

                      @Override
                      public Flux<Payload> requestStream(Payload payload) {
                        return errorOutcome
                            ? Flux.error(new RuntimeException("test"))
                            : Flux.just(payload);
                      }

                      @Override
                      public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
                        return errorOutcome
                            ? Flux.error(new RuntimeException("test"))
                            : Flux.from(payloads);
                      }
                    }))
            .bindNow(LocalServerTransport.create("test"));

    final TestRequestInterceptor testRequestInterceptor = new TestRequestInterceptor();
    final RSocket rSocket =
        RSocketConnector.create()
            .interceptors(
                ir ->
                    ir.forRequestsInRequester(
                        (Function<RSocket, ? extends RequestInterceptor>)
                            (__) -> testRequestInterceptor))
            .connect(LocalClientTransport.create("test", byteBufAllocator))
            .block();

    try {
      rSocket
          .fireAndForget(DefaultPayload.create("test"))
          .onErrorResume(__ -> Mono.empty())
          .block();

      rSocket
          .requestResponse(DefaultPayload.create("test"))
          .onErrorResume(__ -> Mono.empty())
          .block();

      rSocket
          .requestStream(DefaultPayload.create("test"))
          .onErrorResume(__ -> Mono.empty())
          .blockLast();

      rSocket
          .requestChannel(Flux.just(DefaultPayload.create("test")))
          .onErrorResume(__ -> Mono.empty())
          .blockLast();

      testRequestInterceptor
          .expectOnStart(1, FrameType.REQUEST_FNF)
          .expectOnComplete(1)
          .expectOnStart(3, FrameType.REQUEST_RESPONSE)
          .assertNext(
              e ->
                  Assertions.assertThat(e)
                      .hasFieldOrPropertyWithValue("streamId", 3)
                      .hasFieldOrPropertyWithValue(
                          "eventType",
                          errorOutcome
                              ? TestRequestInterceptor.EventType.ON_ERROR
                              : TestRequestInterceptor.EventType.ON_COMPLETE))
          .expectOnStart(5, FrameType.REQUEST_STREAM)
          .assertNext(
              e ->
                  Assertions.assertThat(e)
                      .hasFieldOrPropertyWithValue("streamId", 5)
                      .hasFieldOrPropertyWithValue(
                          "eventType",
                          errorOutcome
                              ? TestRequestInterceptor.EventType.ON_ERROR
                              : TestRequestInterceptor.EventType.ON_COMPLETE))
          .expectOnStart(7, FrameType.REQUEST_CHANNEL)
          .assertNext(
              e ->
                  Assertions.assertThat(e)
                      .hasFieldOrPropertyWithValue("streamId", 7)
                      .hasFieldOrPropertyWithValue(
                          "eventType",
                          errorOutcome
                              ? TestRequestInterceptor.EventType.ON_ERROR
                              : TestRequestInterceptor.EventType.ON_COMPLETE))
          .expectNothing();
    } finally {
      rSocket.dispose();
      closeable.dispose();
      byteBufAllocator.assertHasNoLeaks();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void interceptorShouldBeInstalledProperlyOnTheClientResponderSide(boolean errorOutcome)
      throws InterruptedException {
    final LeaksTrackingByteBufAllocator byteBufAllocator =
        LeaksTrackingByteBufAllocator.instrument(
            ByteBufAllocator.DEFAULT, Duration.ofSeconds(1), "test");

    CountDownLatch latch = new CountDownLatch(1);
    final Closeable closeable =
        RSocketServer.create(
                (setup, rSocket) ->
                    Mono.<RSocket>just(new RSocket() {})
                        .doAfterTerminate(
                            () -> {
                              new Thread(
                                      () -> {
                                        rSocket
                                            .fireAndForget(DefaultPayload.create("test"))
                                            .onErrorResume(__ -> Mono.empty())
                                            .block();

                                        rSocket
                                            .requestResponse(DefaultPayload.create("test"))
                                            .onErrorResume(__ -> Mono.empty())
                                            .block();

                                        rSocket
                                            .requestStream(DefaultPayload.create("test"))
                                            .onErrorResume(__ -> Mono.empty())
                                            .blockLast();

                                        rSocket
                                            .requestChannel(
                                                Flux.just(DefaultPayload.create("test")))
                                            .onErrorResume(__ -> Mono.empty())
                                            .blockLast();
                                        latch.countDown();
                                      })
                                  .start();
                            }))
            .bindNow(LocalServerTransport.create("test"));

    final TestRequestInterceptor testRequestInterceptor = new TestRequestInterceptor();
    final RSocket rSocket =
        RSocketConnector.create()
            .acceptor(
                SocketAcceptor.with(
                    new RSocket() {
                      @Override
                      public Mono<Void> fireAndForget(Payload payload) {
                        return Mono.empty();
                      }

                      @Override
                      public Mono<Payload> requestResponse(Payload payload) {
                        return errorOutcome
                            ? Mono.error(new RuntimeException("test"))
                            : Mono.just(payload);
                      }

                      @Override
                      public Flux<Payload> requestStream(Payload payload) {
                        return errorOutcome
                            ? Flux.error(new RuntimeException("test"))
                            : Flux.just(payload);
                      }

                      @Override
                      public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
                        return errorOutcome
                            ? Flux.error(new RuntimeException("test"))
                            : Flux.from(payloads);
                      }
                    }))
            .interceptors(
                ir ->
                    ir.forRequestsInResponder(
                        (Function<RSocket, ? extends RequestInterceptor>)
                            (__) -> testRequestInterceptor))
            .connect(LocalClientTransport.create("test", byteBufAllocator))
            .block();

    try {
      Assertions.assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();

      testRequestInterceptor
          .expectOnStart(2, FrameType.REQUEST_FNF)
          .expectOnComplete(2)
          .expectOnStart(4, FrameType.REQUEST_RESPONSE)
          .assertNext(
              e ->
                  Assertions.assertThat(e)
                      .hasFieldOrPropertyWithValue("streamId", 4)
                      .hasFieldOrPropertyWithValue(
                          "eventType",
                          errorOutcome
                              ? TestRequestInterceptor.EventType.ON_ERROR
                              : TestRequestInterceptor.EventType.ON_COMPLETE))
          .expectOnStart(6, FrameType.REQUEST_STREAM)
          .assertNext(
              e ->
                  Assertions.assertThat(e)
                      .hasFieldOrPropertyWithValue("streamId", 6)
                      .hasFieldOrPropertyWithValue(
                          "eventType",
                          errorOutcome
                              ? TestRequestInterceptor.EventType.ON_ERROR
                              : TestRequestInterceptor.EventType.ON_COMPLETE))
          .expectOnStart(8, FrameType.REQUEST_CHANNEL)
          .assertNext(
              e ->
                  Assertions.assertThat(e)
                      .hasFieldOrPropertyWithValue("streamId", 8)
                      .hasFieldOrPropertyWithValue(
                          "eventType",
                          errorOutcome
                              ? TestRequestInterceptor.EventType.ON_ERROR
                              : TestRequestInterceptor.EventType.ON_COMPLETE))
          .expectNothing();

    } finally {
      rSocket.dispose();
      closeable.dispose();
      byteBufAllocator.assertHasNoLeaks();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void interceptorShouldBeInstalledProperlyOnTheServerRequesterSide(boolean errorOutcome) {
    final LeaksTrackingByteBufAllocator byteBufAllocator =
        LeaksTrackingByteBufAllocator.instrument(
            ByteBufAllocator.DEFAULT, Duration.ofSeconds(1), "test");

    final TestRequestInterceptor testRequestInterceptor = new TestRequestInterceptor();
    final Closeable closeable =
        RSocketServer.create(
                SocketAcceptor.with(
                    new RSocket() {
                      @Override
                      public Mono<Void> fireAndForget(Payload payload) {
                        return Mono.empty();
                      }

                      @Override
                      public Mono<Payload> requestResponse(Payload payload) {
                        return errorOutcome
                            ? Mono.error(new RuntimeException("test"))
                            : Mono.just(payload);
                      }

                      @Override
                      public Flux<Payload> requestStream(Payload payload) {
                        return errorOutcome
                            ? Flux.error(new RuntimeException("test"))
                            : Flux.just(payload);
                      }

                      @Override
                      public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
                        return errorOutcome
                            ? Flux.error(new RuntimeException("test"))
                            : Flux.from(payloads);
                      }
                    }))
            .interceptors(
                ir ->
                    ir.forRequestsInResponder(
                        (Function<RSocket, ? extends RequestInterceptor>)
                            (__) -> testRequestInterceptor))
            .bindNow(LocalServerTransport.create("test"));
    final RSocket rSocket =
        RSocketConnector.create()
            .connect(LocalClientTransport.create("test", byteBufAllocator))
            .block();

    try {
      rSocket
          .fireAndForget(DefaultPayload.create("test"))
          .onErrorResume(__ -> Mono.empty())
          .block();

      rSocket
          .requestResponse(DefaultPayload.create("test"))
          .onErrorResume(__ -> Mono.empty())
          .block();

      rSocket
          .requestStream(DefaultPayload.create("test"))
          .onErrorResume(__ -> Mono.empty())
          .blockLast();

      rSocket
          .requestChannel(Flux.just(DefaultPayload.create("test")))
          .onErrorResume(__ -> Mono.empty())
          .blockLast();

      testRequestInterceptor
          .expectOnStart(1, FrameType.REQUEST_FNF)
          .expectOnComplete(1)
          .expectOnStart(3, FrameType.REQUEST_RESPONSE)
          .assertNext(
              e ->
                  Assertions.assertThat(e)
                      .hasFieldOrPropertyWithValue("streamId", 3)
                      .hasFieldOrPropertyWithValue(
                          "eventType",
                          errorOutcome
                              ? TestRequestInterceptor.EventType.ON_ERROR
                              : TestRequestInterceptor.EventType.ON_COMPLETE))
          .expectOnStart(5, FrameType.REQUEST_STREAM)
          .assertNext(
              e ->
                  Assertions.assertThat(e)
                      .hasFieldOrPropertyWithValue("streamId", 5)
                      .hasFieldOrPropertyWithValue(
                          "eventType",
                          errorOutcome
                              ? TestRequestInterceptor.EventType.ON_ERROR
                              : TestRequestInterceptor.EventType.ON_COMPLETE))
          .expectOnStart(7, FrameType.REQUEST_CHANNEL)
          .assertNext(
              e ->
                  Assertions.assertThat(e)
                      .hasFieldOrPropertyWithValue("streamId", 7)
                      .hasFieldOrPropertyWithValue(
                          "eventType",
                          errorOutcome
                              ? TestRequestInterceptor.EventType.ON_ERROR
                              : TestRequestInterceptor.EventType.ON_COMPLETE))
          .expectNothing();
    } finally {
      rSocket.dispose();
      closeable.dispose();
      byteBufAllocator.assertHasNoLeaks();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void interceptorShouldBeInstalledProperlyOnTheServerResponderSide(boolean errorOutcome)
      throws InterruptedException {
    final LeaksTrackingByteBufAllocator byteBufAllocator =
        LeaksTrackingByteBufAllocator.instrument(
            ByteBufAllocator.DEFAULT, Duration.ofSeconds(1), "test");

    CountDownLatch latch = new CountDownLatch(1);
    final TestRequestInterceptor testRequestInterceptor = new TestRequestInterceptor();
    final Closeable closeable =
        RSocketServer.create(
                (setup, rSocket) ->
                    Mono.<RSocket>just(new RSocket() {})
                        .doAfterTerminate(
                            () -> {
                              new Thread(
                                      () -> {
                                        rSocket
                                            .fireAndForget(DefaultPayload.create("test"))
                                            .onErrorResume(__ -> Mono.empty())
                                            .block();

                                        rSocket
                                            .requestResponse(DefaultPayload.create("test"))
                                            .onErrorResume(__ -> Mono.empty())
                                            .block();

                                        rSocket
                                            .requestStream(DefaultPayload.create("test"))
                                            .onErrorResume(__ -> Mono.empty())
                                            .blockLast();

                                        rSocket
                                            .requestChannel(
                                                Flux.just(DefaultPayload.create("test")))
                                            .onErrorResume(__ -> Mono.empty())
                                            .blockLast();
                                        latch.countDown();
                                      })
                                  .start();
                            }))
            .interceptors(
                ir ->
                    ir.forRequestsInRequester(
                        (Function<RSocket, ? extends RequestInterceptor>)
                            (__) -> testRequestInterceptor))
            .bindNow(LocalServerTransport.create("test"));
    final RSocket rSocket =
        RSocketConnector.create()
            .acceptor(
                SocketAcceptor.with(
                    new RSocket() {
                      @Override
                      public Mono<Void> fireAndForget(Payload payload) {
                        return errorOutcome
                            ? Mono.error(new RuntimeException("test"))
                            : Mono.empty();
                      }

                      @Override
                      public Mono<Payload> requestResponse(Payload payload) {
                        return errorOutcome
                            ? Mono.error(new RuntimeException("test"))
                            : Mono.just(payload);
                      }

                      @Override
                      public Flux<Payload> requestStream(Payload payload) {
                        return errorOutcome
                            ? Flux.error(new RuntimeException("test"))
                            : Flux.just(payload);
                      }

                      @Override
                      public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
                        return errorOutcome
                            ? Flux.error(new RuntimeException("test"))
                            : Flux.from(payloads);
                      }
                    }))
            .connect(LocalClientTransport.create("test", byteBufAllocator))
            .block();

    try {
      Assertions.assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();

      testRequestInterceptor
          .expectOnStart(2, FrameType.REQUEST_FNF)
          .expectOnComplete(2)
          .expectOnStart(4, FrameType.REQUEST_RESPONSE)
          .assertNext(
              e ->
                  Assertions.assertThat(e)
                      .hasFieldOrPropertyWithValue("streamId", 4)
                      .hasFieldOrPropertyWithValue(
                          "eventType",
                          errorOutcome
                              ? TestRequestInterceptor.EventType.ON_ERROR
                              : TestRequestInterceptor.EventType.ON_COMPLETE))
          .expectOnStart(6, FrameType.REQUEST_STREAM)
          .assertNext(
              e ->
                  Assertions.assertThat(e)
                      .hasFieldOrPropertyWithValue("streamId", 6)
                      .hasFieldOrPropertyWithValue(
                          "eventType",
                          errorOutcome
                              ? TestRequestInterceptor.EventType.ON_ERROR
                              : TestRequestInterceptor.EventType.ON_COMPLETE))
          .expectOnStart(8, FrameType.REQUEST_CHANNEL)
          .assertNext(
              e ->
                  Assertions.assertThat(e)
                      .hasFieldOrPropertyWithValue("streamId", 8)
                      .hasFieldOrPropertyWithValue(
                          "eventType",
                          errorOutcome
                              ? TestRequestInterceptor.EventType.ON_ERROR
                              : TestRequestInterceptor.EventType.ON_COMPLETE))
          .expectNothing();

    } finally {
      rSocket.dispose();
      closeable.dispose();
      byteBufAllocator.assertHasNoLeaks();
    }
  }

  @Test
  void ensuresExceptionInTheInterceptorIsHandledProperly() {
    final LeaksTrackingByteBufAllocator byteBufAllocator =
        LeaksTrackingByteBufAllocator.instrument(
            ByteBufAllocator.DEFAULT, Duration.ofSeconds(1), "test");

    final Closeable closeable =
        RSocketServer.create(
                SocketAcceptor.with(
                    new RSocket() {
                      @Override
                      public Mono<Void> fireAndForget(Payload payload) {
                        return Mono.empty();
                      }

                      @Override
                      public Mono<Payload> requestResponse(Payload payload) {
                        return Mono.just(payload);
                      }

                      @Override
                      public Flux<Payload> requestStream(Payload payload) {
                        return Flux.just(payload);
                      }

                      @Override
                      public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
                        return Flux.from(payloads);
                      }
                    }))
            .bindNow(LocalServerTransport.create("test"));

    final RequestInterceptor testRequestInterceptor =
        new RequestInterceptor() {
          @Override
          public void onStart(int streamId, FrameType requestType, @Nullable ByteBuf metadata) {
            throw new RuntimeException("testOnStart");
          }

          @Override
          public void onTerminate(
              int streamId, FrameType requestType, @Nullable Throwable terminalSignal) {
            throw new RuntimeException("testOnTerminate");
          }

          @Override
          public void onCancel(int streamId, FrameType requestType) {
            throw new RuntimeException("testOnCancel");
          }

          @Override
          public void onReject(
              Throwable rejectionReason, FrameType requestType, @Nullable ByteBuf metadata) {
            throw new RuntimeException("testOnReject");
          }

          @Override
          public void dispose() {}
        };
    final RSocket rSocket =
        RSocketConnector.create()
            .interceptors(
                ir ->
                    ir.forRequestsInRequester(
                        (Function<RSocket, ? extends RequestInterceptor>)
                            (__) -> testRequestInterceptor))
            .connect(LocalClientTransport.create("test", byteBufAllocator))
            .block();

    try {
      StepVerifier.create(rSocket.fireAndForget(DefaultPayload.create("test")))
          .expectSubscription()
          .expectComplete()
          .verify();

      StepVerifier.create(rSocket.requestResponse(DefaultPayload.create("test")))
          .expectSubscription()
          .expectNextCount(1)
          .expectComplete()
          .verify();

      StepVerifier.create(rSocket.requestStream(DefaultPayload.create("test")))
          .expectSubscription()
          .expectNextCount(1)
          .expectComplete()
          .verify();

      StepVerifier.create(rSocket.requestChannel(Flux.just(DefaultPayload.create("test"))))
          .expectSubscription()
          .expectNextCount(1)
          .expectComplete()
          .verify();
    } finally {
      rSocket.dispose();
      closeable.dispose();
      byteBufAllocator.assertHasNoLeaks();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void shouldSupportMultipleInterceptors(boolean errorOutcome) {
    final LeaksTrackingByteBufAllocator byteBufAllocator =
        LeaksTrackingByteBufAllocator.instrument(
            ByteBufAllocator.DEFAULT, Duration.ofSeconds(1), "test");

    final Closeable closeable =
        RSocketServer.create(
                SocketAcceptor.with(
                    new RSocket() {
                      @Override
                      public Mono<Void> fireAndForget(Payload payload) {
                        return Mono.empty();
                      }

                      @Override
                      public Mono<Payload> requestResponse(Payload payload) {
                        return errorOutcome
                            ? Mono.error(new RuntimeException("test"))
                            : Mono.just(payload);
                      }

                      @Override
                      public Flux<Payload> requestStream(Payload payload) {
                        return errorOutcome
                            ? Flux.error(new RuntimeException("test"))
                            : Flux.just(payload);
                      }

                      @Override
                      public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
                        return errorOutcome
                            ? Flux.error(new RuntimeException("test"))
                            : Flux.from(payloads);
                      }
                    }))
            .bindNow(LocalServerTransport.create("test"));

    final RequestInterceptor testRequestInterceptor1 =
        new RequestInterceptor() {
          @Override
          public void onStart(int streamId, FrameType requestType, @Nullable ByteBuf metadata) {
            throw new RuntimeException("testOnStart");
          }

          @Override
          public void onTerminate(
              int streamId, FrameType requestType, @Nullable Throwable terminalSignal) {
            throw new RuntimeException("testOnTerminate");
          }

          @Override
          public void onCancel(int streamId, FrameType requestType) {
            throw new RuntimeException("testOnTerminate");
          }

          @Override
          public void onReject(
              Throwable rejectionReason, FrameType requestType, @Nullable ByteBuf metadata) {
            throw new RuntimeException("testOnReject");
          }

          @Override
          public void dispose() {}
        };
    final TestRequestInterceptor testRequestInterceptor = new TestRequestInterceptor();
    final TestRequestInterceptor testRequestInterceptor2 = new TestRequestInterceptor();
    final RSocket rSocket =
        RSocketConnector.create()
            .interceptors(
                ir ->
                    ir.forRequestsInRequester(
                            (Function<RSocket, ? extends RequestInterceptor>)
                                (__) -> testRequestInterceptor)
                        .forRequestsInRequester(
                            (Function<RSocket, ? extends RequestInterceptor>)
                                (__) -> testRequestInterceptor1)
                        .forRequestsInRequester(
                            (Function<RSocket, ? extends RequestInterceptor>)
                                (__) -> testRequestInterceptor2))
            .connect(LocalClientTransport.create("test", byteBufAllocator))
            .block();

    try {
      rSocket
          .fireAndForget(DefaultPayload.create("test"))
          .onErrorResume(__ -> Mono.empty())
          .block();

      rSocket
          .requestResponse(DefaultPayload.create("test"))
          .onErrorResume(__ -> Mono.empty())
          .block();

      rSocket
          .requestStream(DefaultPayload.create("test"))
          .onErrorResume(__ -> Mono.empty())
          .blockLast();

      rSocket
          .requestChannel(Flux.just(DefaultPayload.create("test")))
          .onErrorResume(__ -> Mono.empty())
          .blockLast();

      testRequestInterceptor
          .expectOnStart(1, FrameType.REQUEST_FNF)
          .expectOnComplete(1)
          .expectOnStart(3, FrameType.REQUEST_RESPONSE)
          .assertNext(
              e ->
                  Assertions.assertThat(e)
                      .hasFieldOrPropertyWithValue("streamId", 3)
                      .hasFieldOrPropertyWithValue(
                          "eventType",
                          errorOutcome
                              ? TestRequestInterceptor.EventType.ON_ERROR
                              : TestRequestInterceptor.EventType.ON_COMPLETE))
          .expectOnStart(5, FrameType.REQUEST_STREAM)
          .assertNext(
              e ->
                  Assertions.assertThat(e)
                      .hasFieldOrPropertyWithValue("streamId", 5)
                      .hasFieldOrPropertyWithValue(
                          "eventType",
                          errorOutcome
                              ? TestRequestInterceptor.EventType.ON_ERROR
                              : TestRequestInterceptor.EventType.ON_COMPLETE))
          .expectOnStart(7, FrameType.REQUEST_CHANNEL)
          .assertNext(
              e ->
                  Assertions.assertThat(e)
                      .hasFieldOrPropertyWithValue("streamId", 7)
                      .hasFieldOrPropertyWithValue(
                          "eventType",
                          errorOutcome
                              ? TestRequestInterceptor.EventType.ON_ERROR
                              : TestRequestInterceptor.EventType.ON_COMPLETE))
          .expectNothing();

      testRequestInterceptor2
          .expectOnStart(1, FrameType.REQUEST_FNF)
          .expectOnComplete(1)
          .expectOnStart(3, FrameType.REQUEST_RESPONSE)
          .assertNext(
              e ->
                  Assertions.assertThat(e)
                      .hasFieldOrPropertyWithValue("streamId", 3)
                      .hasFieldOrPropertyWithValue(
                          "eventType",
                          errorOutcome
                              ? TestRequestInterceptor.EventType.ON_ERROR
                              : TestRequestInterceptor.EventType.ON_COMPLETE))
          .expectOnStart(5, FrameType.REQUEST_STREAM)
          .assertNext(
              e ->
                  Assertions.assertThat(e)
                      .hasFieldOrPropertyWithValue("streamId", 5)
                      .hasFieldOrPropertyWithValue(
                          "eventType",
                          errorOutcome
                              ? TestRequestInterceptor.EventType.ON_ERROR
                              : TestRequestInterceptor.EventType.ON_COMPLETE))
          .expectOnStart(7, FrameType.REQUEST_CHANNEL)
          .assertNext(
              e ->
                  Assertions.assertThat(e)
                      .hasFieldOrPropertyWithValue("streamId", 7)
                      .hasFieldOrPropertyWithValue(
                          "eventType",
                          errorOutcome
                              ? TestRequestInterceptor.EventType.ON_ERROR
                              : TestRequestInterceptor.EventType.ON_COMPLETE))
          .expectNothing();
    } finally {
      rSocket.dispose();
      closeable.dispose();
      byteBufAllocator.assertHasNoLeaks();
    }
  }
}
