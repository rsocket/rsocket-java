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

import io.netty.util.AbstractReferenceCounted;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.frame.FrameType;
import io.rsocket.util.ByteBufPayload;
import io.rsocket.util.DefaultPayload;
import io.rsocket.util.EmptyPayload;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CancellationException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.util.RaceTestUtils;
import reactor.util.retry.Retry;

public class ResolvingRSocketTests {

  @Test
  public void shouldBeSerialIfRacyForFlatMap() {
    for (int i = 0; i < 10000; i++) {
      long[] requested = new long[] {0};
      ResolvingRSocket parent = new ResolvingRSocket(Mono.never());
      Subscription mockSubscription = Mockito.mock(Subscription.class);
      Mockito.doAnswer(a -> requested[0] += (long) a.getArgument(0))
          .when(mockSubscription)
          .request(Mockito.anyLong());
      ResolvingRSocket.FlatMapInner<?> inner =
          new ResolvingRSocket.FlatMapInner<>(parent, EmptyPayload.INSTANCE, FrameType.REQUEST_FNF);

      inner.subscribe(
          new BaseSubscriber<Object>() {
            @Override
            protected void hookOnSubscribe(Subscription subscription) {}
          });

      inner.request(5);

      RaceTestUtils.race(
          () -> inner.onSubscribe(mockSubscription),
          () -> {
            inner.request(10);
            inner.request(10);
            inner.request(10);
          });

      inner.request(15);

      Assertions.assertThat(requested[0]).isEqualTo(50L);
    }
  }

  @Test
  public void shouldBeSerialIfRacyForFlatMapMany() {
    for (int i = 0; i < 10000; i++) {
      long[] requested = new long[] {0};
      ResolvingRSocket parent = new ResolvingRSocket(Mono.never());
      Subscription mockSubscription = Mockito.mock(Subscription.class);
      Mockito.doAnswer(a -> requested[0] += (long) a.getArgument(0))
          .when(mockSubscription)
          .request(Mockito.anyLong());
      ResolvingRSocket.FlatMapManyInner<?> inner =
          new ResolvingRSocket.FlatMapManyInner<>(
              parent, EmptyPayload.INSTANCE, FrameType.REQUEST_STREAM);

      inner.subscribe(
          new BaseSubscriber<Object>() {
            @Override
            protected void hookOnSubscribe(Subscription subscription) {}
          });

      inner.request(5);

      RaceTestUtils.race(
          () -> inner.onSubscribe(mockSubscription),
          () -> {
            inner.request(10);
            inner.request(10);
            inner.request(10);
          });

      inner.request(15);

      Assertions.assertThat(requested[0]).isEqualTo(50L);
    }
  }

  @Test
  public void shouldReleasePayloadIfCancelAndAcceptRacingFlatMapCase() {
    for (int i = 0; i < 10000; i++) {
      RSocket mockRSocket = Mockito.mock(RSocket.class);
      MonoProcessor<Payload> responseMono = MonoProcessor.create();
      Mockito.when(mockRSocket.requestResponse(Mockito.any())).thenReturn(responseMono);
      Mockito.when(mockRSocket.onClose()).thenReturn(Mono.never());
      ResolvingRSocket mockParent = Mockito.mock(ResolvingRSocket.class);
      Payload payload = ByteBufPayload.create("test");
      ResolvingRSocket.FlatMapInner<Payload> flatMapInner =
          new ResolvingRSocket.FlatMapInner<>(mockParent, payload, FrameType.REQUEST_RESPONSE);

      MonoProcessor<Payload> processor = flatMapInner.subscribeWith(MonoProcessor.create());

      RaceTestUtils.race(() -> flatMapInner.accept(mockRSocket, null), processor::cancel);

      if (payload.refCnt() != 0) {
        ArgumentCaptor<ByteBufPayload> payloadArgumentCaptor =
            ArgumentCaptor.forClass(ByteBufPayload.class);
        Mockito.verify(mockRSocket).requestResponse(payloadArgumentCaptor.capture());
        Assertions.assertThat(payloadArgumentCaptor.getValue())
            .matches(AbstractReferenceCounted::release);
      }
    }
  }

  @Test
  public void shouldReleasePayloadIfCancelAndAcceptRacingFlatMapManyCase() {
    for (int i = 0; i < 10000; i++) {
      RSocket mockRSocket = Mockito.mock(RSocket.class);
      MonoProcessor<Payload> responseMono = MonoProcessor.create();
      Mockito.when(mockRSocket.requestStream(Mockito.any())).thenReturn(responseMono.flux());
      Mockito.when(mockRSocket.onClose()).thenReturn(Mono.never());
      ResolvingRSocket mockParent = Mockito.mock(ResolvingRSocket.class);
      Payload payload = ByteBufPayload.create("test");
      ResolvingRSocket.FlatMapManyInner<Payload> flatMapManyInner =
          new ResolvingRSocket.FlatMapManyInner<>(mockParent, payload, FrameType.REQUEST_STREAM);

      MonoProcessor<Payload> processor = flatMapManyInner.subscribeWith(MonoProcessor.create());

      RaceTestUtils.race(() -> flatMapManyInner.accept(mockRSocket, null), processor::cancel);

      if (payload.refCnt() != 0) {
        ArgumentCaptor<ByteBufPayload> payloadArgumentCaptor =
            ArgumentCaptor.forClass(ByteBufPayload.class);
        Mockito.verify(mockRSocket).requestStream(payloadArgumentCaptor.capture());
        Assertions.assertThat(payloadArgumentCaptor.getValue())
            .matches(AbstractReferenceCounted::release);
      }
    }
  }

  @Test
  public void shouldReleasePayloadIfErrorOnAcceptHappensFlatMapCase() {
    RSocket mockRSocket = Mockito.mock(RSocket.class);
    MonoProcessor<Payload> responseMono = MonoProcessor.create();
    Mockito.when(mockRSocket.requestResponse(Mockito.any())).thenReturn(responseMono);
    Mockito.when(mockRSocket.onClose()).thenReturn(Mono.never());
    ResolvingRSocket mockParent = new ResolvingRSocket(Mono.just(mockRSocket));
    Payload payload = ByteBufPayload.create("test");
    ResolvingRSocket.FlatMapInner<Payload> flatMapInner =
        new ResolvingRSocket.FlatMapInner<>(mockParent, payload, FrameType.REQUEST_RESPONSE);

    MonoProcessor<Payload> processor = flatMapInner.subscribeWith(MonoProcessor.create());

    flatMapInner.accept(null, new RuntimeException("test"));

    Assertions.assertThat(payload.refCnt()).isZero();
    StepVerifier.create(processor).expectErrorMessage("test").verify(Duration.ofMillis(100));
  }

  @Test
  public void shouldReleasePayloadIfErrorOnAcceptHappensFlatMapManyCase() {
    RSocket mockRSocket = Mockito.mock(RSocket.class);
    MonoProcessor<Payload> responseMono = MonoProcessor.create();
    Mockito.when(mockRSocket.requestStream(Mockito.any())).thenReturn(responseMono.flux());
    Mockito.when(mockRSocket.onClose()).thenReturn(Mono.never());
    ResolvingRSocket mockParent = new ResolvingRSocket(Mono.just(mockRSocket));
    Payload payload = ByteBufPayload.create("test");
    ResolvingRSocket.FlatMapManyInner<Payload> flatMapManyInner =
        new ResolvingRSocket.FlatMapManyInner<>(mockParent, payload, FrameType.REQUEST_STREAM);

    MonoProcessor<Payload> processor = flatMapManyInner.subscribeWith(MonoProcessor.create());

    flatMapManyInner.accept(null, new RuntimeException("test"));

    Assertions.assertThat(payload.refCnt()).isZero();
    StepVerifier.create(processor).expectErrorMessage("test").verify(Duration.ofMillis(100));
  }

  @Test
  public void shouldExpireValueOnRacingDisposeAndNext() {
    Hooks.onErrorDropped(t -> {});
    Hooks.onNextDropped(System.out::println);
    for (int i = 0; i < 10000; i++) {
      RSocket mockRSocket = Mockito.mock(RSocket.class);
      MonoProcessor<Payload> responseMono = MonoProcessor.create();
      Mockito.when(mockRSocket.requestResponse(EmptyPayload.INSTANCE)).thenReturn(responseMono);
      Mockito.when(mockRSocket.onClose()).thenReturn(Mono.never());
      final CoreSubscriber<? super RSocket>[] monoSubscribers = new CoreSubscriber[1];
      Subscription mockSubscription = Mockito.mock(Subscription.class);
      final Mono<RSocket> stringMono =
          new Mono<RSocket>() {
            @Override
            public void subscribe(CoreSubscriber<? super RSocket> actual) {
              actual.onSubscribe(mockSubscription);
              monoSubscribers[0] = actual;
            }
          };

      final ResolvingRSocket resolvingRSocket = stringMono.as(ResolvingRSocket::new);

      final MonoProcessor<Payload> processor =
          resolvingRSocket
              .requestResponse(EmptyPayload.INSTANCE)
              .subscribeWith(MonoProcessor.create());

      RaceTestUtils.race(() -> monoSubscribers[0].onNext(mockRSocket), resolvingRSocket::dispose);

      Assertions.assertThat(processor.isTerminated()).isTrue();
      Mockito.verify(mockSubscription).cancel();

      responseMono.onNext(EmptyPayload.INSTANCE);
      if (processor.isError()) {
        Assertions.assertThat(processor.getError())
            .isInstanceOf(CancellationException.class)
            .hasMessage("Disposed");
      } else {
        Assertions.assertThat(processor.peek()).isEqualTo(EmptyPayload.INSTANCE);
      }
      Mockito.verify(mockRSocket).dispose();
    }
  }

  @Test
  public void shouldNotifyAllTheSubscribersUnderRacingBetweenSubscribeAndComplete() {
    Hooks.onErrorDropped(t -> {});
    for (int i = 0; i < 10000; i++) {
      RSocket mockRSocket = Mockito.mock(RSocket.class);
      MonoProcessor<Payload> responseMono = MonoProcessor.create();
      Mockito.when(mockRSocket.requestResponse(EmptyPayload.INSTANCE)).thenReturn(responseMono);
      Mockito.when(mockRSocket.onClose()).thenReturn(Mono.never());
      final TestPublisher<RSocket> cold =
          TestPublisher.createNoncompliant(TestPublisher.Violation.REQUEST_OVERFLOW);

      final ResolvingRSocket resolvingRSocket = cold.mono().as(ResolvingRSocket::new);

      final MonoProcessor<Payload> processor =
          resolvingRSocket
              .requestResponse(EmptyPayload.INSTANCE)
              .subscribeWith(MonoProcessor.create());
      final MonoProcessor<Payload> racerProcessor = MonoProcessor.create();

      cold.next(mockRSocket);

      RaceTestUtils.race(
          cold::complete,
          () ->
              resolvingRSocket
                  .requestResponse(EmptyPayload.INSTANCE)
                  .subscribeWith(racerProcessor));

      responseMono.onNext(EmptyPayload.INSTANCE);

      Assertions.assertThat(processor.isTerminated()).isTrue();

      Assertions.assertThat(processor.peek()).isEqualTo(EmptyPayload.INSTANCE);
      Assertions.assertThat(racerProcessor.peek()).isEqualTo(EmptyPayload.INSTANCE);

      Assertions.assertThat(resolvingRSocket.subscribers).isEqualTo(ResolvingRSocket.READY);

      Assertions.assertThat(
              resolvingRSocket.add(
                  new ResolvingRSocket.FlatMapInner<>(
                      resolvingRSocket, EmptyPayload.INSTANCE, FrameType.REQUEST_FNF)))
          .isEqualTo(ResolvingRSocket.READY_STATE);
    }
  }

  @Test
  public void shouldEstablishValueOnceInCaseOfRacingBetweenSubscribers() {
    for (int i = 0; i < 10000; i++) {

      RSocket mockRSocket = Mockito.mock(RSocket.class);
      MonoProcessor<Payload> responseMono = MonoProcessor.create();
      Mockito.when(mockRSocket.requestResponse(EmptyPayload.INSTANCE)).thenReturn(responseMono);
      Mockito.when(mockRSocket.onClose()).thenReturn(Mono.never());

      final TestPublisher<RSocket> cold = TestPublisher.createCold();
      cold.next(mockRSocket);

      final ResolvingRSocket resolvingRSocket = cold.mono().as(ResolvingRSocket::new);

      final MonoProcessor<Payload> processor = MonoProcessor.create();
      final MonoProcessor<Payload> racerProcessor = MonoProcessor.create();

      Assertions.assertThat(cold.subscribeCount()).isZero();

      RaceTestUtils.race(
          () -> resolvingRSocket.requestResponse(EmptyPayload.INSTANCE).subscribe(processor),
          () -> resolvingRSocket.requestResponse(EmptyPayload.INSTANCE).subscribe(racerProcessor));

      responseMono.onNext(EmptyPayload.INSTANCE);

      Assertions.assertThat(processor.isTerminated()).isTrue();
      Assertions.assertThat(racerProcessor.isTerminated()).isTrue();

      Assertions.assertThat(processor.peek()).isEqualTo(EmptyPayload.INSTANCE);
      Assertions.assertThat(racerProcessor.peek()).isEqualTo(EmptyPayload.INSTANCE);

      Assertions.assertThat(resolvingRSocket.subscribers).isEqualTo(ResolvingRSocket.READY);

      Assertions.assertThat(cold.subscribeCount()).isOne();

      Assertions.assertThat(
              resolvingRSocket.add(
                  new ResolvingRSocket.FlatMapManyInner<>(
                      resolvingRSocket, EmptyPayload.INSTANCE, FrameType.REQUEST_STREAM)))
          .isEqualTo(ResolvingRSocket.READY_STATE);
    }
  }

  @Test
  public void shouldExpireValueOnRacingDisposeAndNoValueComplete() {
    Hooks.onErrorDropped(t -> {});
    for (int i = 0; i < 10000; i++) {

      RSocket mockRSocket = Mockito.mock(RSocket.class);
      MonoProcessor<Payload> responseMono = MonoProcessor.create();
      Mockito.when(mockRSocket.requestResponse(EmptyPayload.INSTANCE)).thenReturn(responseMono);
      Mockito.when(mockRSocket.onClose()).thenReturn(Mono.never());

      final TestPublisher<RSocket> cold =
          TestPublisher.createNoncompliant(TestPublisher.Violation.REQUEST_OVERFLOW);

      final ResolvingRSocket resolvingRSocket = cold.mono().as(ResolvingRSocket::new);

      final MonoProcessor<Payload> processor =
          resolvingRSocket
              .requestResponse(EmptyPayload.INSTANCE)
              .subscribeWith(MonoProcessor.create());

      RaceTestUtils.race(cold::complete, resolvingRSocket::dispose);

      Assertions.assertThat(processor.isTerminated()).isTrue();

      Throwable error = processor.getError();

      if (error instanceof CancellationException) {
        Assertions.assertThat(error)
            .isInstanceOf(CancellationException.class)
            .hasMessage("Disposed");
      } else {
        Assertions.assertThat(error)
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Unexpected Completion of the Upstream");
      }

      Mockito.verifyNoInteractions(mockRSocket);
      Assertions.assertThat(resolvingRSocket.isDisposed()).isTrue();
    }
  }

  @Test
  public void shouldExpireValueOnRacingDisposeAndComplete() {
    Hooks.onErrorDropped(t -> {});
    for (int i = 0; i < 10000; i++) {
      RSocket mockRSocket = Mockito.mock(RSocket.class);
      MonoProcessor<Payload> responseMono = MonoProcessor.create();
      Mockito.when(mockRSocket.requestResponse(EmptyPayload.INSTANCE)).thenReturn(responseMono);
      Mockito.when(mockRSocket.onClose()).thenReturn(Mono.never());

      final TestPublisher<RSocket> cold =
          TestPublisher.createNoncompliant(TestPublisher.Violation.REQUEST_OVERFLOW);

      final ResolvingRSocket resolvingRSocket = cold.mono().as(ResolvingRSocket::new);

      final MonoProcessor<Payload> processor =
          resolvingRSocket
              .requestResponse(EmptyPayload.INSTANCE)
              .subscribeWith(MonoProcessor.create());

      cold.next(mockRSocket);

      RaceTestUtils.race(cold::complete, resolvingRSocket::dispose);

      if (processor.isError()) {
        Assertions.assertThat(processor.getError())
            .isInstanceOf(CancellationException.class)
            .hasMessage("Disposed");
      } else {
        responseMono.onNext(EmptyPayload.INSTANCE);
        Assertions.assertThat(processor.peek()).isEqualTo(EmptyPayload.INSTANCE);
      }
      Assertions.assertThat(processor.isTerminated()).isTrue();

      Mockito.verify(mockRSocket).dispose();
    }
  }

  @Test
  public void shouldExpireValueOnRacingDisposeAndError() {
    Hooks.onErrorDropped(t -> {});
    RuntimeException runtimeException = new RuntimeException("test");
    for (int i = 0; i < 10000; i++) {
      RSocket mockRSocket = Mockito.mock(RSocket.class);
      MonoProcessor<Payload> responseMono = MonoProcessor.create();
      Mockito.when(mockRSocket.requestResponse(EmptyPayload.INSTANCE)).thenReturn(responseMono);
      Mockito.when(mockRSocket.onClose()).thenReturn(Mono.never());

      final TestPublisher<RSocket> cold =
          TestPublisher.createNoncompliant(TestPublisher.Violation.REQUEST_OVERFLOW);

      final ResolvingRSocket resolvingRSocket = cold.mono().as(ResolvingRSocket::new);

      final MonoProcessor<Payload> processor =
          resolvingRSocket
              .requestResponse(EmptyPayload.INSTANCE)
              .subscribeWith(MonoProcessor.create());

      cold.next(mockRSocket);

      RaceTestUtils.race(() -> cold.error(runtimeException), resolvingRSocket::dispose);

      if (processor.isError()) {
        if (processor.getError() instanceof CancellationException) {
          Assertions.assertThat(processor.getError())
              .isInstanceOf(CancellationException.class)
              .hasMessage("Disposed");
        } else {
          Assertions.assertThat(processor.getError())
              .isInstanceOf(RuntimeException.class)
              .hasMessage("test");
        }
      } else {
        responseMono.onNext(EmptyPayload.INSTANCE);
        Assertions.assertThat(processor.peek()).isEqualTo(EmptyPayload.INSTANCE);
      }

      Assertions.assertThat(processor.isTerminated()).isTrue();
      Mockito.verify(mockRSocket).dispose();
    }
  }

  @Test
  public void shouldExpireValueOnRacingDisposeAndErrorWithNoBackoff() {
    Hooks.onErrorDropped(t -> {});
    RuntimeException runtimeException = new RuntimeException("test");
    for (int i = 0; i < 10000; i++) {
      RSocket mockRSocket = Mockito.mock(RSocket.class);
      MonoProcessor<Payload> responseMono = MonoProcessor.create();
      Mockito.when(mockRSocket.requestResponse(EmptyPayload.INSTANCE)).thenReturn(responseMono);
      Mockito.when(mockRSocket.onClose()).thenReturn(Mono.never());
      final TestPublisher<RSocket> cold =
          TestPublisher.createNoncompliant(TestPublisher.Violation.REQUEST_OVERFLOW);

      final ResolvingRSocket resolvingRSocket =
          cold.mono()
              .retryWhen(Retry.max(1).filter(t -> t instanceof Exception))
              .as(ResolvingRSocket::new);

      final MonoProcessor<Payload> processor =
          resolvingRSocket
              .requestResponse(EmptyPayload.INSTANCE)
              .subscribeWith(MonoProcessor.create());

      cold.next(mockRSocket);

      RaceTestUtils.race(() -> cold.error(runtimeException), resolvingRSocket::dispose);

      if (processor.isError()) {

        if (processor.getError() instanceof CancellationException) {
          Assertions.assertThat(processor.getError())
              .isInstanceOf(CancellationException.class)
              .hasMessage("Disposed");
        } else {
          Assertions.assertThat(processor.getError())
              .matches(Exceptions::isRetryExhausted)
              .hasCause(runtimeException);
        }
      } else {
        responseMono.onNext(EmptyPayload.INSTANCE);
        Assertions.assertThat(processor.peek()).isEqualTo(EmptyPayload.INSTANCE);
      }

      Assertions.assertThat(processor.isTerminated()).isTrue();
      Mockito.verify(mockRSocket).dispose();
    }
  }

  @Test
  public void shouldNotExpiredIfNotCompleted() {
    RSocket mockRSocket = Mockito.mock(RSocket.class);
    MonoProcessor<Payload> responseMono = MonoProcessor.create();
    Mockito.when(mockRSocket.requestResponse(EmptyPayload.INSTANCE)).thenReturn(responseMono);
    Mockito.when(mockRSocket.onClose()).thenReturn(Mono.never());
    final TestPublisher<RSocket> publisher =
        TestPublisher.createNoncompliant(TestPublisher.Violation.REQUEST_OVERFLOW);

    final ResolvingRSocket resolvingRSocket = publisher.mono().as(ResolvingRSocket::new);

    MonoProcessor<Payload> processor = MonoProcessor.create();

    resolvingRSocket.requestResponse(EmptyPayload.INSTANCE).subscribe(processor);

    Assertions.assertThat(processor.isTerminated()).isFalse();

    publisher.next(mockRSocket);

    Assertions.assertThat(processor.isTerminated()).isFalse();

    resolvingRSocket.invalidate();

    Assertions.assertThat(processor.isTerminated()).isFalse();
    publisher.assertSubscribers(1);
    Assertions.assertThat(publisher.subscribeCount()).isEqualTo(1);

    publisher.complete();
    responseMono.onNext(EmptyPayload.INSTANCE);

    Assertions.assertThat(processor.isTerminated()).isTrue();

    publisher.assertSubscribers(0);
    Assertions.assertThat(publisher.subscribeCount()).isEqualTo(1);
    Mockito.verify(mockRSocket, Mockito.never()).dispose();
  }

  @Test
  public void shouldNotEmitUntilCompletion() {
    RSocket mockRSocket = Mockito.mock(RSocket.class);
    MonoProcessor<Payload> responseMono = MonoProcessor.create();
    Mockito.when(mockRSocket.requestResponse(EmptyPayload.INSTANCE)).thenReturn(responseMono);
    Mockito.when(mockRSocket.onClose()).thenReturn(Mono.never());
    final TestPublisher<RSocket> publisher =
        TestPublisher.createNoncompliant(TestPublisher.Violation.REQUEST_OVERFLOW);

    final ResolvingRSocket resolvingRSocket = publisher.mono().as(ResolvingRSocket::new);

    MonoProcessor<Payload> processor = MonoProcessor.create();

    resolvingRSocket.requestResponse(EmptyPayload.INSTANCE).subscribe(processor);

    Assertions.assertThat(processor.isTerminated()).isFalse();

    publisher.next(mockRSocket);

    Assertions.assertThat(processor.isTerminated()).isFalse();
    responseMono.onNext(EmptyPayload.INSTANCE);

    publisher.complete();

    Assertions.assertThat(processor.isTerminated()).isTrue();
    Assertions.assertThat(processor.peek()).isEqualTo(EmptyPayload.INSTANCE);
  }

  @Test
  public void shouldBePossibleToRemoveThemSelvesFromTheList_CancellationTest() {
    RSocket mockRSocket = Mockito.mock(RSocket.class);
    MonoProcessor<Payload> responseMono = MonoProcessor.create();
    Mockito.when(mockRSocket.requestResponse(EmptyPayload.INSTANCE)).thenReturn(responseMono);
    Mockito.when(mockRSocket.onClose()).thenReturn(Mono.never());
    final TestPublisher<RSocket> publisher =
        TestPublisher.createNoncompliant(TestPublisher.Violation.REQUEST_OVERFLOW);

    final ResolvingRSocket resolvingRSocket = publisher.mono().as(ResolvingRSocket::new);

    MonoProcessor<Payload> processor = MonoProcessor.create();

    resolvingRSocket.requestResponse(EmptyPayload.INSTANCE).subscribe(processor);

    Assertions.assertThat(processor.isTerminated()).isFalse();

    publisher.next(mockRSocket);

    Assertions.assertThat(processor.isTerminated()).isFalse();

    processor.cancel();

    Assertions.assertThat(resolvingRSocket.subscribers)
        .isEqualTo(ResolvingRSocket.EMPTY_SUBSCRIBED);

    publisher.complete();

    Assertions.assertThat(processor.isTerminated()).isFalse();
    Assertions.assertThat(processor.peek()).isNull();
  }

  @Test
  public void shouldExpireValueOnDispose() {
    RSocket mockRSocket = Mockito.mock(RSocket.class);
    MonoProcessor<Payload> responseMono = MonoProcessor.create();
    Mockito.when(mockRSocket.requestResponse(EmptyPayload.INSTANCE)).thenReturn(responseMono);
    Mockito.when(mockRSocket.onClose()).thenReturn(Mono.never());
    final TestPublisher<RSocket> publisher = TestPublisher.create();

    final int timeout = 10;

    final ResolvingRSocket resolvingRSocket = publisher.mono().as(ResolvingRSocket::new);

    StepVerifier.create(resolvingRSocket.requestResponse(EmptyPayload.INSTANCE))
        .expectSubscription()
        .then(() -> publisher.next(mockRSocket))
        .then(() -> responseMono.onNext(DefaultPayload.create("test")))
        .expectNextMatches(p -> p.getDataUtf8().equals("test"))
        .expectComplete()
        .verify(Duration.ofSeconds(timeout));

    resolvingRSocket.dispose();

    Assertions.assertThat(resolvingRSocket.isDisposed()).isTrue();

    Payload payload = ByteBufPayload.create("test");

    StepVerifier.create(resolvingRSocket.requestResponse(payload).subscribeOn(Schedulers.elastic()))
        .expectSubscription()
        .expectError(CancellationException.class)
        .verify(Duration.ofSeconds(timeout));

    Assertions.assertThat(payload.refCnt()).isZero();
  }

  @Test
  public void shouldNotifyAllTheSubscribers() {
    RSocket mockRSocket = Mockito.mock(RSocket.class);
    MonoProcessor<Payload> responseMono = MonoProcessor.create();
    Mockito.when(mockRSocket.fireAndForget(EmptyPayload.INSTANCE)).thenReturn(Mono.empty());
    Mockito.when(mockRSocket.requestResponse(EmptyPayload.INSTANCE)).thenReturn(responseMono);
    Mockito.when(mockRSocket.requestStream(EmptyPayload.INSTANCE)).thenReturn(responseMono.flux());
    Mockito.when(mockRSocket.requestChannel(Mockito.any())).thenReturn(responseMono.flux());
    Mockito.when(mockRSocket.onClose()).thenReturn(Mono.never());
    final TestPublisher<RSocket> publisher = TestPublisher.create();
    // given

    final ResolvingRSocket resolvingRSocket = publisher.mono().as(ResolvingRSocket::new);

    final MonoProcessor<Void> sub1 = MonoProcessor.create();
    final MonoProcessor<Payload> sub2 = MonoProcessor.create();
    final MonoProcessor<Payload> sub3 = MonoProcessor.create();
    final MonoProcessor<Payload> sub4 = MonoProcessor.create();

    resolvingRSocket.fireAndForget(EmptyPayload.INSTANCE).subscribe(sub1);
    resolvingRSocket.requestResponse(EmptyPayload.INSTANCE).subscribe(sub2);
    resolvingRSocket.requestStream(EmptyPayload.INSTANCE).subscribe(sub3);
    resolvingRSocket.requestChannel(Flux.just(EmptyPayload.INSTANCE)).subscribe(sub4);

    Assertions.assertThat(resolvingRSocket.subscribers).hasSize(4);

    final ArrayList<MonoProcessor<Payload>> processors = new ArrayList<>(200);

    for (int i = 0; i < 100; i++) {
      final MonoProcessor<Payload> subA = MonoProcessor.create();
      final MonoProcessor<Payload> subB = MonoProcessor.create();
      processors.add(subA);
      processors.add(subB);
      RaceTestUtils.race(
          () -> resolvingRSocket.requestResponse(EmptyPayload.INSTANCE).subscribe(subA),
          () -> resolvingRSocket.requestResponse(EmptyPayload.INSTANCE).subscribe(subB));
    }

    Assertions.assertThat(resolvingRSocket.subscribers).hasSize(204);

    sub1.dispose();

    Assertions.assertThat(resolvingRSocket.subscribers).hasSize(203);

    publisher.next(mockRSocket);
    responseMono.onNext(EmptyPayload.INSTANCE);

    Assertions.assertThatThrownBy(sub1::peek).isInstanceOf(CancellationException.class);
    Assertions.assertThat(sub2.peek()).isEqualTo(EmptyPayload.INSTANCE);
    Assertions.assertThat(sub3.peek()).isEqualTo(EmptyPayload.INSTANCE);
    Assertions.assertThat(sub4.peek()).isEqualTo(EmptyPayload.INSTANCE);

    for (MonoProcessor<Payload> sub : processors) {
      Assertions.assertThat(sub.peek()).isEqualTo(EmptyPayload.INSTANCE);
      Assertions.assertThat(sub.isTerminated()).isTrue();
    }

    Assertions.assertThat(publisher.subscribeCount()).isEqualTo(1);
  }
}
