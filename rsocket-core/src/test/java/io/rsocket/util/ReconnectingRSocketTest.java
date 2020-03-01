package io.rsocket.util;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.util.RaceTestUtils;

class ReconnectingRSocketTest {

  @SuppressWarnings("unchecked")
  static Stream<BiFunction<RSocket, Mono<Payload>, CorePublisher>> invocations() {
    return Stream.of(
        (rSocket, arg) -> rSocket.fireAndForget(arg.block()),
        (rSocket, arg) -> rSocket.requestResponse(arg.block()),
        (rSocket, arg) -> rSocket.requestStream(arg.block()),
        RSocket::requestChannel);
  }

  private static final Payload TEST_PAYLOAD = DefaultPayload.create("");
  private static final Mono<Payload> MONO_TEST_PAYLOAD = Mono.just(TEST_PAYLOAD);

  @DisplayName("Verifies that subscriptions to the given source RSocket only on method call")
  @ParameterizedTest
  @MethodSource("invocations")
  public void testSubscribesOnFirstMethodCall(
      BiFunction<RSocket, Mono<Payload>, CorePublisher> invocation) {

    RSocket rSocketMock = Mockito.mock(RSocket.class);
    Mockito.when(rSocketMock.fireAndForget(Mockito.any(Payload.class))).thenReturn(Mono.never());
    Mockito.when(rSocketMock.requestResponse(Mockito.any(Payload.class))).thenReturn(Mono.never());
    Mockito.when(rSocketMock.requestStream(Mockito.any(Payload.class))).thenReturn(Flux.never());
    Mockito.when(rSocketMock.requestChannel(Mockito.any())).thenReturn(Flux.never());
    Mockito.when(rSocketMock.onClose()).thenReturn(Mono.never());
    ReconnectingRSocket reconnectingRSocket =
        new ReconnectingRSocket(
            Mono.fromSupplier(() -> rSocketMock), t -> true, Schedulers.parallel(), 1000, 1500);

    Mockito.verifyZeroInteractions(rSocketMock);
    Assertions.assertThat(reconnectingRSocket.value).isNull();

    CorePublisher corePublisher = invocation.apply(reconnectingRSocket, MONO_TEST_PAYLOAD);

    Mockito.verifyZeroInteractions(rSocketMock);
    Assertions.assertThat(reconnectingRSocket.value).isNull();

    if (corePublisher instanceof Mono) {
      ((Mono) corePublisher).subscribe();
    } else {
      ((Flux) corePublisher).subscribe();
    }

    invocation.apply(Mockito.verify(rSocketMock), MONO_TEST_PAYLOAD);
    Assertions.assertThat(reconnectingRSocket.value).isEqualTo(rSocketMock);
    Assertions.assertThat(reconnectingRSocket.subscribers)
        .isEqualTo(ReconnectingRSocket.TERMINATED);
  }

  @DisplayName(
      "Verifies that ReconnectingRSocket reconnect when the source "
          + "RSocketMono is empty or error one")
  @Test
  @SuppressWarnings("unchecked")
  public void testReconnectsWhenGotCompletion() {
    RSocket rSocketMock = Mockito.mock(RSocket.class);
    Mockito.when(rSocketMock.fireAndForget(Mockito.any(Payload.class))).thenReturn(Mono.empty());
    Mockito.when(rSocketMock.onClose()).thenReturn(Mono.never());
    Supplier<Mono<RSocket>> rSocketMonoMock = Mockito.mock(Supplier.class);
    Mockito.when(rSocketMonoMock.get())
        .thenReturn(Mono.error(new RuntimeException()), Mono.empty(), Mono.just(rSocketMock));

    ReconnectingRSocket reconnectingRSocket =
        new ReconnectingRSocket(
            Mono.defer(rSocketMonoMock), t -> true, Schedulers.parallel(), 10, 20);

    Mockito.verifyZeroInteractions(rSocketMock);
    Assertions.assertThat(reconnectingRSocket.value).isNull();

    Mono<Void> fnfMono = reconnectingRSocket.fireAndForget(TEST_PAYLOAD);

    Mockito.verifyZeroInteractions(rSocketMock);
    Assertions.assertThat(reconnectingRSocket.value).isNull();

    StepVerifier.create(fnfMono).verifyComplete();

    Mockito.verify(rSocketMock).fireAndForget(TEST_PAYLOAD);
    Assertions.assertThat(reconnectingRSocket.value).isEqualTo(rSocketMock);
    Assertions.assertThat(reconnectingRSocket.subscribers)
        .isEqualTo(ReconnectingRSocket.TERMINATED);

    Mockito.verify(rSocketMonoMock, Mockito.times(3)).get();
  }

  @DisplayName(
      "Verifies that ReconnectingRSocket reconnect when got reconnectable "
          + "exception in the logical stream")
  @ParameterizedTest
  @MethodSource("invocations")
  @SuppressWarnings("unchecked")
  public void testReconnectsWhenGotLogicalStreamError(
      BiFunction<RSocket, Mono<Payload>, CorePublisher> invocation) {
    RSocket rSocketMock = Mockito.mock(RSocket.class);
    Mockito.when(rSocketMock.fireAndForget(Mockito.any(Payload.class)))
        .thenReturn(
            Mono.<Void>error(new ClosedChannelException())
                .doOnError(e -> Mockito.when(rSocketMock.isDisposed()).thenReturn(true)),
            Mono.empty());
    Mockito.when(rSocketMock.requestResponse(Mockito.any(Payload.class)))
        .thenReturn(
            Mono.<Payload>error(new ClosedChannelException())
                .doOnError(e -> Mockito.when(rSocketMock.isDisposed()).thenReturn(true)),
            Mono.empty());
    Mockito.when(rSocketMock.requestStream(Mockito.any(Payload.class)))
        .thenReturn(
            Flux.<Payload>error(new ClosedChannelException())
                .doOnError(e -> Mockito.when(rSocketMock.isDisposed()).thenReturn(true)),
            Flux.empty());
    Mockito.when(rSocketMock.requestChannel(Mockito.any()))
        .thenReturn(
            Flux.<Payload>error(new ClosedChannelException())
                .doOnError(e -> Mockito.when(rSocketMock.isDisposed()).thenReturn(true)),
            Flux.empty());
    Mockito.when(rSocketMock.onClose()).thenReturn(Mono.never());
    Supplier<Mono<RSocket>> rSocketMonoMock = Mockito.mock(Supplier.class);
    Mockito.when(rSocketMonoMock.get())
        .thenReturn(
            Mono.error(new RuntimeException()),
            Mono.empty(),
            Mono.just(rSocketMock),
            Mono.just(rSocketMock));

    ReconnectingRSocket reconnectingRSocket =
        new ReconnectingRSocket(
            Mono.defer(() -> rSocketMonoMock.get()), t -> true, Schedulers.parallel(), 10, 20);

    Mockito.verifyNoInteractions(rSocketMock);
    Assertions.assertThat(reconnectingRSocket.value).isNull();

    CorePublisher corePublisher = invocation.apply(reconnectingRSocket, MONO_TEST_PAYLOAD);

    Mockito.verifyNoInteractions(rSocketMock);
    Assertions.assertThat(reconnectingRSocket.value).isNull();

    if (corePublisher instanceof Mono) {
      corePublisher = ((Mono) corePublisher).retryBackoff(1, Duration.ofMillis(10));
    } else {
      corePublisher = ((Flux) corePublisher).retryBackoff(1, Duration.ofMillis(10));
    }

    StepVerifier.create(corePublisher).verifyComplete();

    invocation.apply(Mockito.verify(rSocketMock, Mockito.times(2)), MONO_TEST_PAYLOAD);
    Assertions.assertThat(reconnectingRSocket.value).isEqualTo(rSocketMock);
    Assertions.assertThat(reconnectingRSocket.subscribers)
        .isEqualTo(ReconnectingRSocket.TERMINATED);

    Mockito.verify(rSocketMonoMock, Mockito.times(4)).get();
  }

  @Test
  public void racingTest() throws InterruptedException {
    Schedulers.parallel().start();
    AtomicLong subscribedTimes = new AtomicLong();
    MockSupplier mockRSocket = new MockSupplier(subscribedTimes);

    ReconnectingRSocket rSocket = ReconnectingRSocket.builder()
            .withSourceRSocket(mockRSocket)
            .withRetryOnScheduler(Schedulers.parallel())
            .withRetryPeriod(Duration.ofMillis(0))
            .build();

    Assertions.assertThat(subscribedTimes.get()).isZero();

    for (int i = 2; i <= 101; i++) {
      Mono<Payload> instance = rSocket.requestResponse(EmptyPayload.INSTANCE);
      instance.subscribe(new CoreSubscriber<Payload>() {
        @Override
        public void onSubscribe(Subscription s) {
          s.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(Payload payload) {

        }

        @Override
        public void onError(Throwable t) {
          instance.subscribe(this);
        }

        @Override
        public void onComplete() {

        }
      });

      Mono<Payload> instance2 = rSocket.requestResponse(EmptyPayload.INSTANCE);
      instance2.subscribe(new CoreSubscriber<Payload>() {
        @Override
        public void onSubscribe(Subscription s) {
          s.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(Payload payload) {

        }

        @Override
        public void onError(Throwable t) {
          instance2.subscribe(this);
        }

        @Override
        public void onComplete() {

        }
      });

//      System.out.println(i);
      mockRSocket.lastSuppliedRSocket.dispose();
      while (i != subscribedTimes.get()) {
        Thread.sleep(10);
      }
    }

    Assertions.assertThat(subscribedTimes.get()).isEqualTo(101);
  }

  static class MockSupplier implements Supplier<Mono<RSocket>> {

    final AtomicLong subscribedCounted;
    RSocket lastSuppliedRSocket;

    MockSupplier(AtomicLong subscribedCounted) {
      this.subscribedCounted = subscribedCounted;
    }

    public Mono<RSocket> get() {
//      System.out.println("Subscribed");
      lastSuppliedRSocket = new MockRSocket(TestPublisher.create(), MonoProcessor.create());
      subscribedCounted.incrementAndGet();
      return Mono.just(lastSuppliedRSocket).publishOn(Schedulers.parallel());
    }

  }

  static class MockRSocket implements RSocket {

    static final ClosedChannelException ERROR = new ClosedChannelException();

    final TestPublisher testPublisher;
    final MonoProcessor<Void> onDispose;

    MockRSocket(TestPublisher testPublisher, MonoProcessor<Void> onDispose) {
      this.testPublisher = testPublisher;
      this.onDispose = onDispose;
    }

    @Override
    public Mono<Void> fireAndForget(Payload payload) {
      return testPublisher.mono().publishOn(Schedulers.parallel());
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
      return testPublisher.mono().publishOn(Schedulers.parallel());
    }

    @Override
    public Flux<Payload> requestStream(Payload payload) {
      return testPublisher.flux().publishOn(Schedulers.parallel());
    }

    @Override
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
      return Flux.from(payloads)
              .switchOnFirst((s, f) -> testPublisher.flux());
    }

    @Override
    public Mono<Void> metadataPush(Payload payload) {
      return testPublisher.mono().publishOn(Schedulers.parallel());
    }

    @Override
    public double availability() {
      return 1.0D;
    }

    @Override
    public Mono<Void> onClose() {
      return onDispose;
    }

    @Override
    public void dispose() {
//      printStackTrace();
      RaceTestUtils.race(onDispose::onComplete, () -> testPublisher.error(ERROR), Schedulers.elastic());
    }

    @Override
    public boolean isDisposed() {
      return onDispose.isDisposed();
    }

    private void printStackTrace() {
      StringBuilder s = new StringBuilder();
      StackTraceElement[] trace = Thread.currentThread().getStackTrace();
      for (StackTraceElement traceElement : trace) {
        s.append("\tat " + traceElement + "\n\r");
        if (traceElement.toString().contains("io.rsocket.util.ReconnectingRSocketTest.racingTest")) {
          break;
        }
      }

      System.err.println(s);
    }
  }
}
