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

package io.rsocket.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.rsocket.RaceTestConstants;
import io.rsocket.internal.subscriber.AssertSubscriber;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.util.RaceTestUtils;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;
import reactor.util.retry.Retry;

public class ReconnectMonoTests {

  private Queue<Retry.RetrySignal> retries = new ConcurrentLinkedQueue<>();
  private Queue<Tuple2<Object, Invalidatable>> received = new ConcurrentLinkedQueue<>();
  private Queue<Object> expired = new ConcurrentLinkedQueue<>();

  @Test
  public void shouldExpireValueOnRacingDisposeAndNext() {
    Hooks.onErrorDropped(t -> {});
    Hooks.onNextDropped(System.out::println);
    for (int i = 0; i < RaceTestConstants.REPEATS; i++) {
      final int index = i;
      final CoreSubscriber<? super String>[] monoSubscribers = new CoreSubscriber[1];
      Subscription mockSubscription = Mockito.mock(Subscription.class);
      final Mono<String> stringMono =
          new Mono<String>() {
            @Override
            public void subscribe(CoreSubscriber<? super String> actual) {
              actual.onSubscribe(mockSubscription);
              monoSubscribers[0] = actual;
            }
          };

      final ReconnectMono<String> reconnectMono =
          stringMono
              .doOnDiscard(Object.class, System.out::println)
              .as(source -> new ReconnectMono<>(source, onExpire(), onValue()));

      final AssertSubscriber<String> subscriber =
          reconnectMono.subscribeWith(new AssertSubscriber<>());

      assertThat(expired).isEmpty();
      assertThat(received).isEmpty();

      RaceTestUtils.race(() -> monoSubscribers[0].onNext("value" + index), reconnectMono::dispose);

      monoSubscribers[0].onComplete();

      subscriber.assertTerminated();
      Mockito.verify(mockSubscription).cancel();

      if (!subscriber.errors().isEmpty()) {
        subscriber
            .assertError(CancellationException.class)
            .assertErrorMessage("ReconnectMono has already been disposed");

        assertThat(expired).containsOnly("value" + i);
      } else {
        subscriber.assertValues("value" + i);
      }

      expired.clear();
      received.clear();
    }
  }

  @Test
  public void shouldNotifyAllTheSubscribersUnderRacingBetweenSubscribeAndComplete() {
    Hooks.onErrorDropped(t -> {});
    for (int i = 0; i < RaceTestConstants.REPEATS; i++) {
      final TestPublisher<String> cold =
          TestPublisher.createNoncompliant(TestPublisher.Violation.REQUEST_OVERFLOW);

      final ReconnectMono<String> reconnectMono =
          cold.mono().as(source -> new ReconnectMono<>(source, onExpire(), onValue()));

      final AssertSubscriber<String> subscriber =
          reconnectMono.subscribeWith(new AssertSubscriber<>());
      final AssertSubscriber<String> raceSubscriber = new AssertSubscriber<>();

      assertThat(expired).isEmpty();
      assertThat(received).isEmpty();

      cold.next("value" + i);

      RaceTestUtils.race(cold::complete, () -> reconnectMono.subscribe(raceSubscriber));

      subscriber.assertTerminated();
      subscriber.assertValues("value" + i);
      raceSubscriber.assertValues("value" + i);

      assertThat(reconnectMono.resolvingInner.subscribers).isEqualTo(ResolvingOperator.READY);

      assertThat(
              reconnectMono.resolvingInner.add(
                  new ResolvingOperator.MonoDeferredResolutionOperator<>(
                      reconnectMono.resolvingInner, subscriber)))
          .isEqualTo(ResolvingOperator.READY_STATE);

      assertThat(expired).isEmpty();
      assertThat(received).hasSize(1).containsOnly(Tuples.of("value" + i, reconnectMono));

      received.clear();
    }
  }

  @Test
  public void shouldNotExpireNewlyResolvedValueIfSubscribeIsRacingWithInvalidate() {
    Hooks.onErrorDropped(t -> {});
    for (int i = 0; i < RaceTestConstants.REPEATS; i++) {
      final int index = i;
      final TestPublisher<String> cold =
          TestPublisher.createNoncompliant(TestPublisher.Violation.REQUEST_OVERFLOW);

      final ReconnectMono<String> reconnectMono =
          cold.mono().as(source -> new ReconnectMono<>(source, onExpire(), onValue()));

      final AssertSubscriber<String> subscriber =
          reconnectMono.subscribeWith(new AssertSubscriber<>());
      final AssertSubscriber<String> raceSubscriber = new AssertSubscriber<>();

      assertThat(expired).isEmpty();
      assertThat(received).isEmpty();

      reconnectMono.resolvingInner.mainSubscriber.onNext("value_to_expire" + i);
      reconnectMono.resolvingInner.mainSubscriber.onComplete();

      RaceTestUtils.race(
          reconnectMono::invalidate,
          () -> {
            reconnectMono.subscribe(raceSubscriber);
            if (!raceSubscriber.isTerminated()) {
              reconnectMono.resolvingInner.mainSubscriber.onNext("value_to_not_expire" + index);
              reconnectMono.resolvingInner.mainSubscriber.onComplete();
            }
          });

      subscriber.assertTerminated();
      subscriber.assertValues("value_to_expire" + i);

      raceSubscriber.assertComplete();
      String v = raceSubscriber.values().get(0);
      if (reconnectMono.resolvingInner.subscribers == ResolvingOperator.READY) {
        assertThat(v).isEqualTo("value_to_not_expire" + index);
      } else {
        assertThat(v).isEqualTo("value_to_expire" + index);
      }

      assertThat(expired).hasSize(1).containsOnly("value_to_expire" + i);
      if (reconnectMono.resolvingInner.subscribers == ResolvingOperator.READY) {
        assertThat(received)
            .hasSize(2)
            .containsExactly(
                Tuples.of("value_to_expire" + i, reconnectMono),
                Tuples.of("value_to_not_expire" + i, reconnectMono));
      } else {
        assertThat(received)
            .hasSize(1)
            .containsOnly(Tuples.of("value_to_expire" + i, reconnectMono));
      }

      expired.clear();
      received.clear();
    }
  }

  @Test
  public void shouldNotExpireNewlyResolvedValueIfSubscribeIsRacingWithInvalidates() {
    Hooks.onErrorDropped(t -> {});
    for (int i = 0; i < RaceTestConstants.REPEATS; i++) {
      final int index = i;
      final TestPublisher<String> cold =
          TestPublisher.createNoncompliant(TestPublisher.Violation.REQUEST_OVERFLOW);

      final ReconnectMono<String> reconnectMono =
          cold.mono().as(source -> new ReconnectMono<>(source, onExpire(), onValue()));

      final AssertSubscriber<String> subscriber =
          reconnectMono.subscribeWith(new AssertSubscriber<>());
      final AssertSubscriber<String> raceSubscriber = new AssertSubscriber<>();

      assertThat(expired).isEmpty();
      assertThat(received).isEmpty();

      reconnectMono.resolvingInner.mainSubscriber.onNext("value_to_expire" + i);
      reconnectMono.resolvingInner.mainSubscriber.onComplete();

      RaceTestUtils.race(
          reconnectMono::invalidate,
          reconnectMono::invalidate,
          () -> {
            reconnectMono.subscribe(raceSubscriber);
            if (!raceSubscriber.isTerminated()) {
              reconnectMono.resolvingInner.mainSubscriber.onNext(
                  "value_to_possibly_expire" + index);
              reconnectMono.resolvingInner.mainSubscriber.onComplete();
            }
          });

      subscriber.assertTerminated();
      subscriber.assertValues("value_to_expire" + i);

      raceSubscriber.assertComplete();
      assertThat(raceSubscriber.values().get(0))
          .isIn("value_to_possibly_expire" + index, "value_to_expire" + index);

      if (expired.size() == 2) {
        assertThat(expired)
            .hasSize(2)
            .containsExactly("value_to_expire" + i, "value_to_possibly_expire" + i);
      } else {
        assertThat(expired).hasSize(1).containsOnly("value_to_expire" + i);
      }
      if (received.size() == 2) {
        assertThat(received)
            .hasSize(2)
            .containsExactly(
                Tuples.of("value_to_expire" + i, reconnectMono),
                Tuples.of("value_to_possibly_expire" + i, reconnectMono));
      } else {
        assertThat(received)
            .hasSize(1)
            .containsOnly(Tuples.of("value_to_expire" + i, reconnectMono));
      }

      expired.clear();
      received.clear();
    }
  }

  @Test
  public void shouldNotExpireNewlyResolvedValueIfBlockIsRacingWithInvalidate() {
    Hooks.onErrorDropped(t -> {});
    for (int i = 0; i < RaceTestConstants.REPEATS; i++) {
      final int index = i;
      final Mono<String> source =
          Mono.fromSupplier(
              new Supplier<String>() {
                boolean once = false;

                @Override
                public String get() {

                  if (!once) {
                    once = true;
                    return "value_to_expire" + index;
                  }

                  return "value_to_not_expire" + index;
                }
              });

      final ReconnectMono<String> reconnectMono =
          new ReconnectMono<>(
              source.subscribeOn(Schedulers.boundedElastic()), onExpire(), onValue());

      assertThat(expired).isEmpty();
      assertThat(received).isEmpty();

      final AssertSubscriber<String> subscriber =
          reconnectMono.subscribeWith(new AssertSubscriber<>());

      subscriber.await().assertComplete();

      assertThat(expired).isEmpty();

      RaceTestUtils.race(
          () ->
              assertThat(reconnectMono.block())
                  .matches(
                      (v) ->
                          v.equals("value_to_not_expire" + index)
                              || v.equals("value_to_expire" + index)),
          reconnectMono::invalidate);

      subscriber.assertTerminated();

      subscriber.assertValues("value_to_expire" + i);

      assertThat(expired).hasSize(1).containsOnly("value_to_expire" + i);
      if (reconnectMono.resolvingInner.subscribers == ResolvingOperator.READY) {
        await().atMost(Duration.ofSeconds(5)).until(() -> received.size() == 2);
        assertThat(received)
            .hasSize(2)
            .containsExactly(
                Tuples.of("value_to_expire" + i, reconnectMono),
                Tuples.of("value_to_not_expire" + i, reconnectMono));
      } else {
        assertThat(received)
            .hasSize(1)
            .containsOnly(Tuples.of("value_to_expire" + i, reconnectMono));
      }

      expired.clear();
      received.clear();
    }
  }

  @Test
  public void shouldEstablishValueOnceInCaseOfRacingBetweenSubscribers() {
    for (int i = 0; i < RaceTestConstants.REPEATS; i++) {
      final TestPublisher<String> cold = TestPublisher.createCold();
      cold.next("value" + i);

      final ReconnectMono<String> reconnectMono =
          cold.mono().as(source -> new ReconnectMono<>(source, onExpire(), onValue()));

      final AssertSubscriber<String> subscriber = new AssertSubscriber<>();
      final AssertSubscriber<String> raceSubscriber = new AssertSubscriber<>();

      assertThat(expired).isEmpty();
      assertThat(received).isEmpty();

      assertThat(cold.subscribeCount()).isZero();

      RaceTestUtils.race(
          () -> reconnectMono.subscribe(subscriber), () -> reconnectMono.subscribe(raceSubscriber));

      subscriber.assertTerminated();
      assertThat(raceSubscriber.isTerminated()).isTrue();

      subscriber.assertValues("value" + i);
      raceSubscriber.assertValues("value" + i);

      assertThat(reconnectMono.resolvingInner.subscribers).isEqualTo(ResolvingOperator.READY);

      assertThat(cold.subscribeCount()).isOne();

      assertThat(
              reconnectMono.resolvingInner.add(
                  new ResolvingOperator.MonoDeferredResolutionOperator<>(
                      reconnectMono.resolvingInner, subscriber)))
          .isEqualTo(ResolvingOperator.READY_STATE);

      assertThat(expired).isEmpty();
      assertThat(received).hasSize(1).containsOnly(Tuples.of("value" + i, reconnectMono));

      received.clear();
    }
  }

  @Test
  public void shouldEstablishValueOnceInCaseOfRacingBetweenSubscribeAndBlock() {
    Duration timeout = Duration.ofMillis(100);
    for (int i = 0; i < RaceTestConstants.REPEATS; i++) {
      final TestPublisher<String> cold = TestPublisher.createCold();
      cold.next("value" + i);

      final ReconnectMono<String> reconnectMono =
          cold.mono().as(source -> new ReconnectMono<>(source, onExpire(), onValue()));

      final AssertSubscriber<String> subscriber = new AssertSubscriber<>();

      assertThat(expired).isEmpty();
      assertThat(received).isEmpty();

      assertThat(cold.subscribeCount()).isZero();

      String[] values = new String[1];

      RaceTestUtils.race(
          () -> values[0] = reconnectMono.block(timeout),
          () -> reconnectMono.subscribe(subscriber));

      subscriber.assertTerminated();

      subscriber.assertValues("value" + i);
      assertThat(values).containsExactly("value" + i);

      assertThat(reconnectMono.resolvingInner.subscribers).isEqualTo(ResolvingOperator.READY);

      assertThat(cold.subscribeCount()).isOne();

      assertThat(
              reconnectMono.resolvingInner.add(
                  new ResolvingOperator.MonoDeferredResolutionOperator<>(
                      reconnectMono.resolvingInner, subscriber)))
          .isEqualTo(ResolvingOperator.READY_STATE);

      assertThat(expired).isEmpty();
      assertThat(received).hasSize(1).containsOnly(Tuples.of("value" + i, reconnectMono));

      received.clear();
    }
  }

  @Test
  public void shouldEstablishValueOnceInCaseOfRacingBetweenBlocks() {
    Duration timeout = Duration.ofMillis(100);
    for (int i = 0; i < RaceTestConstants.REPEATS; i++) {
      final TestPublisher<String> cold = TestPublisher.createCold();
      cold.next("value" + i);

      final ReconnectMono<String> reconnectMono =
          cold.mono().as(source -> new ReconnectMono<>(source, onExpire(), onValue()));

      assertThat(expired).isEmpty();
      assertThat(received).isEmpty();

      assertThat(cold.subscribeCount()).isZero();

      String[] values1 = new String[1];
      String[] values2 = new String[1];

      RaceTestUtils.race(
          () -> values1[0] = reconnectMono.block(timeout),
          () -> values2[0] = reconnectMono.block(timeout));

      assertThat(values2).containsExactly("value" + i);
      assertThat(values1).containsExactly("value" + i);

      assertThat(reconnectMono.resolvingInner.subscribers).isEqualTo(ResolvingOperator.READY);

      assertThat(cold.subscribeCount()).isOne();

      assertThat(
              reconnectMono.resolvingInner.add(
                  new ResolvingOperator.MonoDeferredResolutionOperator<>(
                      reconnectMono.resolvingInner, new AssertSubscriber<>())))
          .isEqualTo(ResolvingOperator.READY_STATE);

      assertThat(expired).isEmpty();
      assertThat(received).hasSize(1).containsOnly(Tuples.of("value" + i, reconnectMono));

      received.clear();
    }
  }

  @Test
  public void shouldExpireValueOnRacingDisposeAndNoValueComplete() {
    Hooks.onErrorDropped(t -> {});
    for (int i = 0; i < RaceTestConstants.REPEATS; i++) {
      final TestPublisher<String> cold =
          TestPublisher.createNoncompliant(TestPublisher.Violation.REQUEST_OVERFLOW);

      final ReconnectMono<String> reconnectMono =
          cold.mono().as(source -> new ReconnectMono<>(source, onExpire(), onValue()));

      final AssertSubscriber<String> subscriber =
          reconnectMono.subscribeWith(new AssertSubscriber<>());

      assertThat(expired).isEmpty();
      assertThat(received).isEmpty();

      RaceTestUtils.race(cold::complete, reconnectMono::dispose);

      subscriber.assertTerminated();

      Throwable error = subscriber.errors().get(0);

      if (error instanceof CancellationException) {
        assertThat(error)
            .isInstanceOf(CancellationException.class)
            .hasMessage("ReconnectMono has already been disposed");
      } else {
        assertThat(error)
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Source completed empty");
      }

      assertThat(expired).isEmpty();

      expired.clear();
      received.clear();
    }
  }

  @Test
  public void shouldExpireValueOnRacingDisposeAndComplete() {
    Hooks.onErrorDropped(t -> {});
    for (int i = 0; i < RaceTestConstants.REPEATS; i++) {
      final TestPublisher<String> cold =
          TestPublisher.createNoncompliant(TestPublisher.Violation.REQUEST_OVERFLOW);

      final ReconnectMono<String> reconnectMono =
          cold.mono().as(source -> new ReconnectMono<>(source, onExpire(), onValue()));

      final AssertSubscriber<String> subscriber =
          reconnectMono.subscribeWith(new AssertSubscriber<>());

      assertThat(expired).isEmpty();
      assertThat(received).isEmpty();

      cold.next("value" + i);

      RaceTestUtils.race(cold::complete, reconnectMono::dispose);

      subscriber.assertTerminated();

      if (!subscriber.errors().isEmpty()) {
        assertThat(subscriber.errors().get(0))
            .isInstanceOf(CancellationException.class)
            .hasMessage("ReconnectMono has already been disposed");
      } else {
        assertThat(received).hasSize(1).containsOnly(Tuples.of("value" + i, reconnectMono));
        subscriber.assertValues("value" + i);
      }

      assertThat(expired).hasSize(1).containsOnly("value" + i);

      expired.clear();
      received.clear();
    }
  }

  @Test
  public void shouldExpireValueOnRacingDisposeAndError() {
    Hooks.onErrorDropped(t -> {});
    RuntimeException runtimeException = new RuntimeException("test");
    for (int i = 0; i < RaceTestConstants.REPEATS; i++) {
      final TestPublisher<String> cold =
          TestPublisher.createNoncompliant(TestPublisher.Violation.REQUEST_OVERFLOW);

      final ReconnectMono<String> reconnectMono =
          cold.mono().as(source -> new ReconnectMono<>(source, onExpire(), onValue()));

      final AssertSubscriber<String> subscriber =
          reconnectMono.subscribeWith(new AssertSubscriber<>());

      assertThat(expired).isEmpty();
      assertThat(received).isEmpty();

      cold.next("value" + i);

      RaceTestUtils.race(() -> cold.error(runtimeException), reconnectMono::dispose);

      subscriber.assertTerminated();

      if (!subscriber.errors().isEmpty()) {
        Throwable error = subscriber.errors().get(0);
        if (error instanceof CancellationException) {
          assertThat(error)
              .isInstanceOf(CancellationException.class)
              .hasMessage("ReconnectMono has already been disposed");
        } else {
          assertThat(error).isInstanceOf(RuntimeException.class).hasMessage("test");
        }
      } else {
        assertThat(received).hasSize(1).containsOnly(Tuples.of("value" + i, reconnectMono));
        subscriber.assertValues("value" + i);
      }

      assertThat(expired).hasSize(1).containsOnly("value" + i);

      expired.clear();
      received.clear();
    }
  }

  @Test
  public void shouldExpireValueOnRacingDisposeAndErrorWithNoBackoff() {
    Hooks.onErrorDropped(t -> {});
    RuntimeException runtimeException = new RuntimeException("test");
    for (int i = 0; i < RaceTestConstants.REPEATS; i++) {
      final TestPublisher<String> cold =
          TestPublisher.createNoncompliant(TestPublisher.Violation.REQUEST_OVERFLOW);

      final ReconnectMono<String> reconnectMono =
          cold.mono()
              .retryWhen(Retry.max(1).filter(t -> t instanceof Exception))
              .as(source -> new ReconnectMono<>(source, onExpire(), onValue()));

      final AssertSubscriber<String> subscriber =
          reconnectMono.subscribeWith(new AssertSubscriber<>());

      assertThat(expired).isEmpty();
      assertThat(received).isEmpty();

      cold.next("value" + i);

      RaceTestUtils.race(() -> cold.error(runtimeException), reconnectMono::dispose);

      subscriber.assertTerminated();

      if (!subscriber.errors().isEmpty()) {
        Throwable error = subscriber.errors().get(0);
        if (error instanceof CancellationException) {
          assertThat(error)
              .isInstanceOf(CancellationException.class)
              .hasMessage("ReconnectMono has already been disposed");
        } else {
          assertThat(error).matches(Exceptions::isRetryExhausted).hasCause(runtimeException);
        }

        assertThat(expired).hasSize(1).containsOnly("value" + i);
      } else {
        assertThat(received).hasSize(1).containsOnly(Tuples.of("value" + i, reconnectMono));
        subscriber.assertValues("value" + i);
      }

      expired.clear();
      received.clear();
    }
  }

  @Test
  public void shouldThrowOnBlocking() {
    final TestPublisher<String> publisher =
        TestPublisher.createNoncompliant(TestPublisher.Violation.REQUEST_OVERFLOW);

    final ReconnectMono<String> reconnectMono =
        publisher.mono().as(source -> new ReconnectMono<>(source, onExpire(), onValue()));

    Assertions.assertThatThrownBy(() -> reconnectMono.block(Duration.ofMillis(100)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Timeout on Mono blocking read");
  }

  @Test
  public void shouldThrowOnBlockingIfHasAlreadyTerminated() {
    final TestPublisher<String> publisher =
        TestPublisher.createNoncompliant(TestPublisher.Violation.REQUEST_OVERFLOW);

    final ReconnectMono<String> reconnectMono =
        publisher.mono().as(source -> new ReconnectMono<>(source, onExpire(), onValue()));

    publisher.error(new RuntimeException("test"));

    Assertions.assertThatThrownBy(() -> reconnectMono.block(Duration.ofMillis(100)))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("test")
        .hasSuppressedException(new Exception("Terminated with an error"));
  }

  @Test
  public void shouldBeScannable() {
    final TestPublisher<String> publisher =
        TestPublisher.createNoncompliant(TestPublisher.Violation.REQUEST_OVERFLOW);

    final Mono<String> parent = publisher.mono();
    final ReconnectMono<String> reconnectMono =
        parent.as(source -> new ReconnectMono<>(source, onExpire(), onValue()));

    final Scannable scannableOfReconnect = Scannable.from(reconnectMono);

    assertThat(
            (List)
                scannableOfReconnect.parents().map(s -> s.getClass()).collect(Collectors.toList()))
        .hasSize(1)
        .containsExactly(publisher.mono().getClass());
    assertThat(scannableOfReconnect.scanUnsafe(Scannable.Attr.TERMINATED)).isEqualTo(false);
    assertThat(scannableOfReconnect.scanUnsafe(Scannable.Attr.ERROR)).isNull();

    final AssertSubscriber<String> subscriber =
        reconnectMono.subscribeWith(new AssertSubscriber<>());

    final Scannable scannableOfMonoProcessor = Scannable.from(subscriber);

    assertThat(
            (List)
                scannableOfMonoProcessor
                    .parents()
                    .map(s -> s.getClass())
                    .collect(Collectors.toList()))
        .hasSize(4)
        .containsExactly(
            ResolvingOperator.MonoDeferredResolutionOperator.class,
            ReconnectMono.ResolvingInner.class,
            ReconnectMono.class,
            publisher.mono().getClass());

    reconnectMono.dispose();

    assertThat(scannableOfReconnect.scanUnsafe(Scannable.Attr.TERMINATED)).isEqualTo(true);
    assertThat(scannableOfReconnect.scanUnsafe(Scannable.Attr.ERROR))
        .isInstanceOf(CancellationException.class);
  }

  @Test
  public void shouldNotExpiredIfNotCompleted() {
    final TestPublisher<String> publisher =
        TestPublisher.createNoncompliant(TestPublisher.Violation.REQUEST_OVERFLOW);

    final ReconnectMono<String> reconnectMono =
        publisher.mono().as(source -> new ReconnectMono<>(source, onExpire(), onValue()));

    AssertSubscriber<String> subscriber = new AssertSubscriber<>();

    reconnectMono.subscribe(subscriber);

    assertThat(expired).isEmpty();
    assertThat(received).isEmpty();
    assertThat(subscriber.isTerminated()).isFalse();

    publisher.next("test");

    assertThat(expired).isEmpty();
    assertThat(received).isEmpty();
    assertThat(subscriber.isTerminated()).isFalse();

    reconnectMono.invalidate();

    assertThat(expired).isEmpty();
    assertThat(received).isEmpty();
    assertThat(subscriber.isTerminated()).isFalse();
    publisher.assertSubscribers(1);
    assertThat(publisher.subscribeCount()).isEqualTo(1);

    publisher.complete();

    assertThat(expired).isEmpty();
    assertThat(received).hasSize(1);
    subscriber.assertTerminated();

    publisher.assertSubscribers(0);
    assertThat(publisher.subscribeCount()).isEqualTo(1);
  }

  @Test
  public void shouldNotEmitUntilCompletion() {
    final TestPublisher<String> publisher =
        TestPublisher.createNoncompliant(TestPublisher.Violation.REQUEST_OVERFLOW);

    final ReconnectMono<String> reconnectMono =
        publisher.mono().as(source -> new ReconnectMono<>(source, onExpire(), onValue()));

    AssertSubscriber<String> subscriber = new AssertSubscriber<>();

    reconnectMono.subscribe(subscriber);

    assertThat(expired).isEmpty();
    assertThat(received).isEmpty();
    assertThat(subscriber.isTerminated()).isFalse();

    publisher.next("test");

    assertThat(expired).isEmpty();
    assertThat(received).isEmpty();
    assertThat(subscriber.isTerminated()).isFalse();

    publisher.complete();

    assertThat(expired).isEmpty();
    assertThat(received).hasSize(1);
    subscriber.assertTerminated();
    subscriber.assertValues("test");
  }

  @Test
  public void shouldBePossibleToRemoveThemSelvesFromTheList_CancellationTest() {
    final TestPublisher<String> publisher =
        TestPublisher.createNoncompliant(TestPublisher.Violation.REQUEST_OVERFLOW);

    final ReconnectMono<String> reconnectMono =
        publisher.mono().as(source -> new ReconnectMono<>(source, onExpire(), onValue()));

    AssertSubscriber<String> subscriber = new AssertSubscriber<>();

    reconnectMono.subscribe(subscriber);

    assertThat(expired).isEmpty();
    assertThat(received).isEmpty();
    assertThat(subscriber.isTerminated()).isFalse();

    publisher.next("test");

    assertThat(expired).isEmpty();
    assertThat(received).isEmpty();
    assertThat(subscriber.isTerminated()).isFalse();

    subscriber.cancel();

    assertThat(reconnectMono.resolvingInner.subscribers)
        .isEqualTo(ResolvingOperator.EMPTY_SUBSCRIBED);

    publisher.complete();

    assertThat(expired).isEmpty();
    assertThat(received).hasSize(1);
    assertThat(subscriber.values()).isEmpty();
  }

  @Test
  public void shouldExpireValueOnDispose() {
    final TestPublisher<String> publisher = TestPublisher.create();
    // given
    final int timeout = 10;

    final ReconnectMono<String> reconnectMono =
        publisher.mono().as(source -> new ReconnectMono<>(source, onExpire(), onValue()));

    StepVerifier.create(reconnectMono)
        .expectSubscription()
        .then(() -> publisher.next("value"))
        .expectNext("value")
        .expectComplete()
        .verify(Duration.ofSeconds(timeout));

    assertThat(expired).isEmpty();
    assertThat(received).hasSize(1);

    reconnectMono.dispose();

    assertThat(expired).hasSize(1);
    assertThat(received).hasSize(1);
    assertThat(reconnectMono.isDisposed()).isTrue();

    StepVerifier.create(reconnectMono.subscribeOn(Schedulers.boundedElastic()))
        .expectSubscription()
        .expectError(CancellationException.class)
        .verify(Duration.ofSeconds(timeout));
  }

  @Test
  public void shouldNotifyAllTheSubscribers() {
    final TestPublisher<String> publisher = TestPublisher.create();

    final ReconnectMono<String> reconnectMono =
        publisher.mono().as(source -> new ReconnectMono<>(source, onExpire(), onValue()));

    final AssertSubscriber<String> sub1 = new AssertSubscriber<>();
    final AssertSubscriber<String> sub2 = new AssertSubscriber<>();
    final AssertSubscriber<String> sub3 = new AssertSubscriber<>();
    final AssertSubscriber<String> sub4 = new AssertSubscriber<>();

    reconnectMono.subscribe(sub1);
    reconnectMono.subscribe(sub2);
    reconnectMono.subscribe(sub3);
    reconnectMono.subscribe(sub4);

    assertThat(reconnectMono.resolvingInner.subscribers).hasSize(4);

    final ArrayList<AssertSubscriber<String>> subscribers = new ArrayList<>(200);

    for (int i = 0; i < RaceTestConstants.REPEATS; i++) {
      final AssertSubscriber<String> subA = new AssertSubscriber<>();
      final AssertSubscriber<String> subB = new AssertSubscriber<>();
      subscribers.add(subA);
      subscribers.add(subB);
      RaceTestUtils.race(() -> reconnectMono.subscribe(subA), () -> reconnectMono.subscribe(subB));
    }

    assertThat(reconnectMono.resolvingInner.subscribers).hasSize(RaceTestConstants.REPEATS * 2 + 4);

    sub1.cancel();

    assertThat(reconnectMono.resolvingInner.subscribers).hasSize(RaceTestConstants.REPEATS * 2 + 3);

    publisher.next("value");

    assertThat(sub1.scan(Scannable.Attr.CANCELLED)).isTrue();
    assertThat(sub2.values().get(0)).isEqualTo("value");
    assertThat(sub3.values().get(0)).isEqualTo("value");
    assertThat(sub4.values().get(0)).isEqualTo("value");

    for (AssertSubscriber<String> sub : subscribers) {
      assertThat(sub.values().get(0)).isEqualTo("value");
      assertThat(sub.isTerminated()).isTrue();
    }

    assertThat(publisher.subscribeCount()).isEqualTo(1);
  }

  @Test
  public void shouldExpireValueExactlyOnceOnRacingBetweenInvalidates() {
    for (int i = 0; i < RaceTestConstants.REPEATS; i++) {
      final TestPublisher<String> cold = TestPublisher.createCold();
      cold.next("value");
      cold.complete();
      final int timeout = 10;

      final ReconnectMono<String> reconnectMono =
          cold.flux()
              .takeLast(1)
              .next()
              .as(source -> new ReconnectMono<>(source, onExpire(), onValue()));

      StepVerifier.create(reconnectMono.subscribeOn(Schedulers.boundedElastic()))
          .expectSubscription()
          .expectNext("value")
          .expectComplete()
          .verify(Duration.ofSeconds(timeout));

      assertThat(expired).isEmpty();
      assertThat(received).hasSize(1).containsOnly(Tuples.of("value", reconnectMono));

      RaceTestUtils.race(reconnectMono::invalidate, reconnectMono::invalidate);

      assertThat(expired).hasSize(1).containsOnly("value");
      assertThat(received).hasSize(1).containsOnly(Tuples.of("value", reconnectMono));

      cold.next("value2");

      StepVerifier.create(reconnectMono.subscribeOn(Schedulers.boundedElastic()))
          .expectSubscription()
          .expectNext("value2")
          .expectComplete()
          .verify(Duration.ofSeconds(timeout));

      assertThat(expired).hasSize(1).containsOnly("value");
      assertThat(received)
          .hasSize(2)
          .containsOnly(Tuples.of("value", reconnectMono), Tuples.of("value2", reconnectMono));

      assertThat(cold.subscribeCount()).isEqualTo(2);

      expired.clear();
      received.clear();
    }
  }

  @Test
  public void shouldExpireValueExactlyOnceOnRacingBetweenInvalidateAndDispose() {
    for (int i = 0; i < RaceTestConstants.REPEATS; i++) {
      final TestPublisher<String> cold = TestPublisher.createCold();
      cold.next("value");
      final int timeout = 10000;

      final ReconnectMono<String> reconnectMono =
          cold.mono().as(source -> new ReconnectMono<>(source, onExpire(), onValue()));

      StepVerifier.create(reconnectMono.subscribeOn(Schedulers.boundedElastic()))
          .expectSubscription()
          .expectNext("value")
          .expectComplete()
          .verify(Duration.ofSeconds(timeout));

      assertThat(expired).isEmpty();
      assertThat(received).hasSize(1).containsOnly(Tuples.of("value", reconnectMono));

      RaceTestUtils.race(reconnectMono::invalidate, reconnectMono::dispose);

      assertThat(expired).hasSize(1).containsOnly("value");
      assertThat(received).hasSize(1).containsOnly(Tuples.of("value", reconnectMono));

      StepVerifier.create(reconnectMono.subscribeOn(Schedulers.boundedElastic()))
          .expectSubscription()
          .expectError(CancellationException.class)
          .verify(Duration.ofSeconds(timeout));

      assertThat(expired).hasSize(1).containsOnly("value");
      assertThat(received).hasSize(1).containsOnly(Tuples.of("value", reconnectMono));

      assertThat(cold.subscribeCount()).isEqualTo(1);

      expired.clear();
      received.clear();
    }
  }

  @Test
  public void shouldTimeoutRetryWithVirtualTime() {
    // given
    final int minBackoff = 1;
    final int maxBackoff = 5;
    final int timeout = 10;

    // then
    StepVerifier.withVirtualTime(
            () ->
                Mono.<String>error(new RuntimeException("Something went wrong"))
                    .retryWhen(
                        Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(minBackoff))
                            .doAfterRetry(onRetry())
                            .maxBackoff(Duration.ofSeconds(maxBackoff)))
                    .timeout(Duration.ofSeconds(timeout))
                    .as(m -> new ReconnectMono<>(m, onExpire(), onValue()))
                    .subscribeOn(Schedulers.boundedElastic()))
        .expectSubscription()
        .thenAwait(Duration.ofSeconds(timeout))
        .expectError(TimeoutException.class)
        .verify(Duration.ofSeconds(timeout));

    assertThat(received).isEmpty();
    assertThat(expired).isEmpty();
  }

  @Test
  public void ensuresThatMainSubscriberAllowsOnlyTerminationWithValue() {
    final int timeout = 10;
    final ReconnectMono<String> reconnectMono =
        new ReconnectMono<>(Mono.empty(), onExpire(), onValue());

    StepVerifier.create(reconnectMono.subscribeOn(Schedulers.boundedElastic()))
        .expectSubscription()
        .expectErrorSatisfies(
            t ->
                assertThat(t)
                    .hasMessage("Source completed empty")
                    .isInstanceOf(IllegalStateException.class))
        .verify(Duration.ofSeconds(timeout));
  }

  @Test
  public void monoRetryNoBackoff() {
    Mono<?> mono =
        Mono.error(new IOException())
            .retryWhen(Retry.max(2).doAfterRetry(onRetry()))
            .as(m -> new ReconnectMono<>(m, onExpire(), onValue()));

    StepVerifier.create(mono).verifyErrorMatches(Exceptions::isRetryExhausted);
    assertRetries(IOException.class, IOException.class);

    assertThat(received).isEmpty();
    assertThat(expired).isEmpty();
  }

  @Test
  public void monoRetryFixedBackoff() {
    Mono<?> mono =
        Mono.error(new IOException())
            .retryWhen(Retry.fixedDelay(1, Duration.ofMillis(500)).doAfterRetry(onRetry()))
            .as(m -> new ReconnectMono<>(m, onExpire(), onValue()));

    StepVerifier.withVirtualTime(() -> mono)
        .expectSubscription()
        .expectNoEvent(Duration.ofMillis(300))
        .thenAwait(Duration.ofMillis(300))
        .verifyErrorMatches(Exceptions::isRetryExhausted);

    assertRetries(IOException.class);

    assertThat(received).isEmpty();
    assertThat(expired).isEmpty();
  }

  @Test
  public void monoRetryExponentialBackoff() {
    Mono<?> mono =
        Mono.error(new IOException())
            .retryWhen(
                Retry.backoff(4, Duration.ofMillis(100))
                    .maxBackoff(Duration.ofMillis(500))
                    .jitter(0.0d)
                    .doAfterRetry(onRetry()))
            .as(m -> new ReconnectMono<>(m, onExpire(), onValue()));

    StepVerifier.withVirtualTime(() -> mono)
        .expectSubscription()
        .thenAwait(Duration.ofMillis(100))
        .thenAwait(Duration.ofMillis(200))
        .thenAwait(Duration.ofMillis(400))
        .thenAwait(Duration.ofMillis(500))
        .verifyErrorMatches(Exceptions::isRetryExhausted);

    assertRetries(IOException.class, IOException.class, IOException.class, IOException.class);

    assertThat(received).isEmpty();
    assertThat(expired).isEmpty();
  }

  Consumer<Retry.RetrySignal> onRetry() {
    return context -> retries.add(context);
  }

  <T> BiConsumer<T, Invalidatable> onValue() {
    return (v, __) -> received.add(Tuples.of(v, __));
  }

  <T> Consumer<T> onExpire() {
    return (v) -> expired.add(v);
  }

  @SafeVarargs
  private final void assertRetries(Class<? extends Throwable>... exceptions) {
    assertThat(retries.size()).isEqualTo(exceptions.length);
    int index = 0;
    for (Iterator<Retry.RetrySignal> it = retries.iterator(); it.hasNext(); ) {
      Retry.RetrySignal retryContext = it.next();
      assertThat(retryContext.totalRetries()).isEqualTo(index);
      assertThat(retryContext.failure().getClass()).isEqualTo(exceptions[index]);
      index++;
    }
  }
}
