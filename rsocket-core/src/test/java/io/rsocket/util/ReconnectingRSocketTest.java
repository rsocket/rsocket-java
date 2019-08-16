package io.rsocket.util;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import reactor.core.CorePublisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

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

    Mockito.verifyZeroInteractions(rSocketMock);
    Assertions.assertThat(reconnectingRSocket.value).isNull();

    CorePublisher corePublisher = invocation.apply(reconnectingRSocket, MONO_TEST_PAYLOAD);

    Mockito.verifyZeroInteractions(rSocketMock);
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
}
