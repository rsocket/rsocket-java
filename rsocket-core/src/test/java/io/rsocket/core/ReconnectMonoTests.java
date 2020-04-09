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

import static org.junit.Assert.assertEquals;

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
import java.util.stream.Collectors;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
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
    for (int i = 0; i < 100000; i++) {
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

      final MonoProcessor<String> processor = reconnectMono.subscribeWith(MonoProcessor.create());

      Assertions.assertThat(expired).isEmpty();
      Assertions.assertThat(received).isEmpty();

      RaceTestUtils.race(() -> monoSubscribers[0].onNext("value" + index), reconnectMono::dispose);

      Assertions.assertThat(processor.isTerminated()).isTrue();
      Mockito.verify(mockSubscription).cancel();

      if (processor.isError()) {
        Assertions.assertThat(processor.getError())
            .isInstanceOf(CancellationException.class)
            .hasMessage("ReconnectMono has already been disposed");

        Assertions.assertThat(expired).containsOnly("value" + i);
      } else {
        Assertions.assertThat(processor.peek()).isEqualTo("value" + i);
      }

      expired.clear();
      received.clear();
    }
  }

  @Test
  public void shouldNotifyAllTheSubscribersUnderRacingBetweenSubscribeAndComplete() {
    Hooks.onErrorDropped(t -> {});
    for (int i = 0; i < 100000; i++) {
      final TestPublisher<String> cold =
          TestPublisher.createNoncompliant(TestPublisher.Violation.REQUEST_OVERFLOW);

      final ReconnectMono<String> reconnectMono =
          cold.mono().as(source -> new ReconnectMono<>(source, onExpire(), onValue()));

      final MonoProcessor<String> processor = reconnectMono.subscribeWith(MonoProcessor.create());
      final MonoProcessor<String> racerProcessor = MonoProcessor.create();

      Assertions.assertThat(expired).isEmpty();
      Assertions.assertThat(received).isEmpty();

      cold.next("value" + i);

      RaceTestUtils.race(cold::complete, () -> reconnectMono.subscribe(racerProcessor));

      Assertions.assertThat(processor.isTerminated()).isTrue();

      Assertions.assertThat(processor.peek()).isEqualTo("value" + i);
      Assertions.assertThat(racerProcessor.peek()).isEqualTo("value" + i);

      Assertions.assertThat(reconnectMono.subscribers).isEqualTo(ReconnectMono.READY);

      Assertions.assertThat(
              reconnectMono.add(new ReconnectMono.ReconnectInner<>(processor, reconnectMono)))
          .isEqualTo(ReconnectMono.READY_STATE);

      Assertions.assertThat(expired).isEmpty();
      Assertions.assertThat(received)
          .hasSize(1)
          .containsOnly(Tuples.of("value" + i, reconnectMono));

      received.clear();
    }
  }

  @Test
  public void shouldEstablishValueOnceInCaseOfRacingBetweenSubscribers() {
    for (int i = 0; i < 100000; i++) {
      final TestPublisher<String> cold = TestPublisher.createCold();
      cold.next("value" + i);

      final ReconnectMono<String> reconnectMono =
          cold.mono().as(source -> new ReconnectMono<>(source, onExpire(), onValue()));

      final MonoProcessor<String> processor = MonoProcessor.create();
      final MonoProcessor<String> racerProcessor = MonoProcessor.create();

      Assertions.assertThat(expired).isEmpty();
      Assertions.assertThat(received).isEmpty();

      Assertions.assertThat(cold.subscribeCount()).isZero();

      RaceTestUtils.race(
          () -> reconnectMono.subscribe(processor), () -> reconnectMono.subscribe(racerProcessor));

      Assertions.assertThat(processor.isTerminated()).isTrue();
      Assertions.assertThat(racerProcessor.isTerminated()).isTrue();

      Assertions.assertThat(processor.peek()).isEqualTo("value" + i);
      Assertions.assertThat(racerProcessor.peek()).isEqualTo("value" + i);

      Assertions.assertThat(reconnectMono.subscribers).isEqualTo(ReconnectMono.READY);

      Assertions.assertThat(cold.subscribeCount()).isOne();

      Assertions.assertThat(
              reconnectMono.add(new ReconnectMono.ReconnectInner<>(processor, reconnectMono)))
          .isEqualTo(ReconnectMono.READY_STATE);

      Assertions.assertThat(expired).isEmpty();
      Assertions.assertThat(received)
          .hasSize(1)
          .containsOnly(Tuples.of("value" + i, reconnectMono));

      received.clear();
    }
  }

  @Test
  public void shouldEstablishValueOnceInCaseOfRacingBetweenSubscribeAndBlock() {
    Duration timeout = Duration.ofMillis(100);
    for (int i = 0; i < 100000; i++) {
      final TestPublisher<String> cold = TestPublisher.createCold();
      cold.next("value" + i);

      final ReconnectMono<String> reconnectMono =
          cold.mono().as(source -> new ReconnectMono<>(source, onExpire(), onValue()));

      final MonoProcessor<String> processor = MonoProcessor.create();

      Assertions.assertThat(expired).isEmpty();
      Assertions.assertThat(received).isEmpty();

      Assertions.assertThat(cold.subscribeCount()).isZero();

      String[] values = new String[1];

      RaceTestUtils.race(
          () -> values[0] = reconnectMono.block(timeout), () -> reconnectMono.subscribe(processor));

      Assertions.assertThat(processor.isTerminated()).isTrue();

      Assertions.assertThat(processor.peek()).isEqualTo("value" + i);
      Assertions.assertThat(values).containsExactly("value" + i);

      Assertions.assertThat(reconnectMono.subscribers).isEqualTo(ReconnectMono.READY);

      Assertions.assertThat(cold.subscribeCount()).isOne();

      Assertions.assertThat(
              reconnectMono.add(new ReconnectMono.ReconnectInner<>(processor, reconnectMono)))
          .isEqualTo(ReconnectMono.READY_STATE);

      Assertions.assertThat(expired).isEmpty();
      Assertions.assertThat(received)
          .hasSize(1)
          .containsOnly(Tuples.of("value" + i, reconnectMono));

      received.clear();
    }
  }

  @Test
  public void shouldEstablishValueOnceInCaseOfRacingBetweenBlocks() {
    Duration timeout = Duration.ofMillis(100);
    for (int i = 0; i < 100000; i++) {
      final TestPublisher<String> cold = TestPublisher.createCold();
      cold.next("value" + i);

      final ReconnectMono<String> reconnectMono =
          cold.mono().as(source -> new ReconnectMono<>(source, onExpire(), onValue()));

      Assertions.assertThat(expired).isEmpty();
      Assertions.assertThat(received).isEmpty();

      Assertions.assertThat(cold.subscribeCount()).isZero();

      String[] values1 = new String[1];
      String[] values2 = new String[1];

      RaceTestUtils.race(
          () -> values1[0] = reconnectMono.block(timeout),
          () -> values2[0] = reconnectMono.block(timeout));

      Assertions.assertThat(values2).containsExactly("value" + i);
      Assertions.assertThat(values1).containsExactly("value" + i);

      Assertions.assertThat(reconnectMono.subscribers).isEqualTo(ReconnectMono.READY);

      Assertions.assertThat(cold.subscribeCount()).isOne();

      Assertions.assertThat(
              reconnectMono.add(
                  new ReconnectMono.ReconnectInner<>(MonoProcessor.create(), reconnectMono)))
          .isEqualTo(ReconnectMono.READY_STATE);

      Assertions.assertThat(expired).isEmpty();
      Assertions.assertThat(received)
          .hasSize(1)
          .containsOnly(Tuples.of("value" + i, reconnectMono));

      received.clear();
    }
  }

  @Test
  public void shouldExpireValueOnRacingDisposeAndNoValueComplete() {
    Hooks.onErrorDropped(t -> {});
    for (int i = 0; i < 100000; i++) {
      final TestPublisher<String> cold =
          TestPublisher.createNoncompliant(TestPublisher.Violation.REQUEST_OVERFLOW);

      final ReconnectMono<String> reconnectMono =
          cold.mono().as(source -> new ReconnectMono<>(source, onExpire(), onValue()));

      final MonoProcessor<String> processor = reconnectMono.subscribeWith(MonoProcessor.create());

      Assertions.assertThat(expired).isEmpty();
      Assertions.assertThat(received).isEmpty();

      RaceTestUtils.race(cold::complete, reconnectMono::dispose);

      Assertions.assertThat(processor.isTerminated()).isTrue();

      Throwable error = processor.getError();

      if (error instanceof CancellationException) {
        Assertions.assertThat(error)
            .isInstanceOf(CancellationException.class)
            .hasMessage("ReconnectMono has already been disposed");
      } else {
        Assertions.assertThat(error)
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Unexpected Completion of the Upstream");
      }

      Assertions.assertThat(expired).isEmpty();

      expired.clear();
      received.clear();
    }
  }

  @Test
  public void shouldExpireValueOnRacingDisposeAndComplete() {
    Hooks.onErrorDropped(t -> {});
    for (int i = 0; i < 100000; i++) {
      final TestPublisher<String> cold =
          TestPublisher.createNoncompliant(TestPublisher.Violation.REQUEST_OVERFLOW);

      final ReconnectMono<String> reconnectMono =
          cold.mono().as(source -> new ReconnectMono<>(source, onExpire(), onValue()));

      final MonoProcessor<String> processor = reconnectMono.subscribeWith(MonoProcessor.create());

      Assertions.assertThat(expired).isEmpty();
      Assertions.assertThat(received).isEmpty();

      cold.next("value" + i);

      RaceTestUtils.race(cold::complete, reconnectMono::dispose);

      Assertions.assertThat(processor.isTerminated()).isTrue();

      if (processor.isError()) {
        Assertions.assertThat(processor.getError())
            .isInstanceOf(CancellationException.class)
            .hasMessage("ReconnectMono has already been disposed");
      } else {
        Assertions.assertThat(received)
            .hasSize(1)
            .containsOnly(Tuples.of("value" + i, reconnectMono));
        Assertions.assertThat(processor.peek()).isEqualTo("value" + i);
      }

      Assertions.assertThat(expired).hasSize(1).containsOnly("value" + i);

      expired.clear();
      received.clear();
    }
  }

  @Test
  public void shouldExpireValueOnRacingDisposeAndError() {
    Hooks.onErrorDropped(t -> {});
    RuntimeException runtimeException = new RuntimeException("test");
    for (int i = 0; i < 100000; i++) {
      final TestPublisher<String> cold =
          TestPublisher.createNoncompliant(TestPublisher.Violation.REQUEST_OVERFLOW);

      final ReconnectMono<String> reconnectMono =
          cold.mono().as(source -> new ReconnectMono<>(source, onExpire(), onValue()));

      final MonoProcessor<String> processor = reconnectMono.subscribeWith(MonoProcessor.create());

      Assertions.assertThat(expired).isEmpty();
      Assertions.assertThat(received).isEmpty();

      cold.next("value" + i);

      RaceTestUtils.race(() -> cold.error(runtimeException), reconnectMono::dispose);

      Assertions.assertThat(processor.isTerminated()).isTrue();

      if (processor.isError()) {
        if (processor.getError() instanceof CancellationException) {
          Assertions.assertThat(processor.getError())
              .isInstanceOf(CancellationException.class)
              .hasMessage("ReconnectMono has already been disposed");
        } else {
          Assertions.assertThat(processor.getError())
              .isInstanceOf(RuntimeException.class)
              .hasMessage("test");
        }
      } else {
        Assertions.assertThat(received)
            .hasSize(1)
            .containsOnly(Tuples.of("value" + i, reconnectMono));
        Assertions.assertThat(processor.peek()).isEqualTo("value" + i);
      }

      Assertions.assertThat(expired).hasSize(1).containsOnly("value" + i);

      expired.clear();
      received.clear();
    }
  }

  @Test
  public void shouldExpireValueOnRacingDisposeAndErrorWithNoBackoff() {
    Hooks.onErrorDropped(t -> {});
    RuntimeException runtimeException = new RuntimeException("test");
    for (int i = 0; i < 100000; i++) {
      final TestPublisher<String> cold =
          TestPublisher.createNoncompliant(TestPublisher.Violation.REQUEST_OVERFLOW);

      final ReconnectMono<String> reconnectMono =
          cold.mono()
              .retryWhen(Retry.max(1).filter(t -> t instanceof Exception))
              .as(source -> new ReconnectMono<>(source, onExpire(), onValue()));

      final MonoProcessor<String> processor = reconnectMono.subscribeWith(MonoProcessor.create());

      Assertions.assertThat(expired).isEmpty();
      Assertions.assertThat(received).isEmpty();

      cold.next("value" + i);

      RaceTestUtils.race(() -> cold.error(runtimeException), reconnectMono::dispose);

      Assertions.assertThat(processor.isTerminated()).isTrue();

      if (processor.isError()) {

        if (processor.getError() instanceof CancellationException) {
          Assertions.assertThat(processor.getError())
              .isInstanceOf(CancellationException.class)
              .hasMessage("ReconnectMono has already been disposed");
        } else {
          Assertions.assertThat(processor.getError())
              .matches(t -> Exceptions.isRetryExhausted(t))
              .hasCause(runtimeException);
        }

        Assertions.assertThat(expired).hasSize(1).containsOnly("value" + i);
      } else {
        Assertions.assertThat(received)
            .hasSize(1)
            .containsOnly(Tuples.of("value" + i, reconnectMono));
        Assertions.assertThat(processor.peek()).isEqualTo("value" + i);
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
        .hasSuppressedException(new Exception("ReconnectMono terminated with an error"));
  }

  @Test
  public void shouldBeScannable() {
    final TestPublisher<String> publisher =
        TestPublisher.createNoncompliant(TestPublisher.Violation.REQUEST_OVERFLOW);

    final Mono<String> parent = publisher.mono();
    final ReconnectMono<String> reconnectMono =
        parent.as(source -> new ReconnectMono<>(source, onExpire(), onValue()));

    final Scannable scannableOfReconnect = Scannable.from(reconnectMono);

    Assertions.assertThat(
            (List)
                scannableOfReconnect.parents().map(s -> s.getClass()).collect(Collectors.toList()))
        .hasSize(1)
        .containsExactly(publisher.mono().getClass());
    Assertions.assertThat(scannableOfReconnect.scanUnsafe(Scannable.Attr.TERMINATED))
        .isEqualTo(false);
    Assertions.assertThat(scannableOfReconnect.scanUnsafe(Scannable.Attr.ERROR)).isNull();

    final MonoProcessor<String> processor = reconnectMono.subscribeWith(MonoProcessor.create());

    final Scannable scannableOfMonoProcessor = Scannable.from(processor);

    Assertions.assertThat(
            (List)
                scannableOfMonoProcessor
                    .parents()
                    .map(s -> s.getClass())
                    .collect(Collectors.toList()))
        .hasSize(3)
        .containsExactly(
            ReconnectMono.ReconnectInner.class, ReconnectMono.class, publisher.mono().getClass());

    reconnectMono.dispose();

    Assertions.assertThat(scannableOfReconnect.scanUnsafe(Scannable.Attr.TERMINATED))
        .isEqualTo(true);
    Assertions.assertThat(scannableOfReconnect.scanUnsafe(Scannable.Attr.ERROR))
        .isInstanceOf(CancellationException.class);
  }

  @Test
  public void shouldNotExpiredIfNotCompleted() {
    final TestPublisher<String> publisher =
        TestPublisher.createNoncompliant(TestPublisher.Violation.REQUEST_OVERFLOW);

    final ReconnectMono<String> reconnectMono =
        publisher.mono().as(source -> new ReconnectMono<>(source, onExpire(), onValue()));

    MonoProcessor<String> processor = MonoProcessor.create();

    reconnectMono.subscribe(processor);

    Assertions.assertThat(expired).isEmpty();
    Assertions.assertThat(received).isEmpty();
    Assertions.assertThat(processor.isTerminated()).isFalse();

    publisher.next("test");

    Assertions.assertThat(expired).isEmpty();
    Assertions.assertThat(received).isEmpty();
    Assertions.assertThat(processor.isTerminated()).isFalse();

    reconnectMono.invalidate();

    Assertions.assertThat(expired).isEmpty();
    Assertions.assertThat(received).isEmpty();
    Assertions.assertThat(processor.isTerminated()).isFalse();
    publisher.assertSubscribers(1);
    Assertions.assertThat(publisher.subscribeCount()).isEqualTo(1);

    publisher.complete();

    Assertions.assertThat(expired).isEmpty();
    Assertions.assertThat(received).hasSize(1);
    Assertions.assertThat(processor.isTerminated()).isTrue();

    publisher.assertSubscribers(0);
    Assertions.assertThat(publisher.subscribeCount()).isEqualTo(1);
  }

  @Test
  public void shouldNotEmitUntilCompletion() {
    final TestPublisher<String> publisher =
        TestPublisher.createNoncompliant(TestPublisher.Violation.REQUEST_OVERFLOW);

    final ReconnectMono<String> reconnectMono =
        publisher.mono().as(source -> new ReconnectMono<>(source, onExpire(), onValue()));

    MonoProcessor<String> processor = MonoProcessor.create();

    reconnectMono.subscribe(processor);

    Assertions.assertThat(expired).isEmpty();
    Assertions.assertThat(received).isEmpty();
    Assertions.assertThat(processor.isTerminated()).isFalse();

    publisher.next("test");

    Assertions.assertThat(expired).isEmpty();
    Assertions.assertThat(received).isEmpty();
    Assertions.assertThat(processor.isTerminated()).isFalse();

    publisher.complete();

    Assertions.assertThat(expired).isEmpty();
    Assertions.assertThat(received).hasSize(1);
    Assertions.assertThat(processor.isTerminated()).isTrue();
    Assertions.assertThat(processor.peek()).isEqualTo("test");
  }

  @Test
  public void shouldBePossibleToRemoveThemSelvesFromTheList_CancellationTest() {
    final TestPublisher<String> publisher =
        TestPublisher.createNoncompliant(TestPublisher.Violation.REQUEST_OVERFLOW);
    // given
    final int minBackoff = 1;
    final int maxBackoff = 5;
    final int timeout = 10;

    final ReconnectMono<String> reconnectMono =
        publisher.mono().as(source -> new ReconnectMono<>(source, onExpire(), onValue()));

    MonoProcessor<String> processor = MonoProcessor.create();

    reconnectMono.subscribe(processor);

    Assertions.assertThat(expired).isEmpty();
    Assertions.assertThat(received).isEmpty();
    Assertions.assertThat(processor.isTerminated()).isFalse();

    publisher.next("test");

    Assertions.assertThat(expired).isEmpty();
    Assertions.assertThat(received).isEmpty();
    Assertions.assertThat(processor.isTerminated()).isFalse();

    processor.cancel();

    Assertions.assertThat(reconnectMono.subscribers).isEqualTo(ReconnectMono.EMPTY_SUBSCRIBED);

    publisher.complete();

    Assertions.assertThat(expired).isEmpty();
    Assertions.assertThat(received).hasSize(1);
    Assertions.assertThat(processor.isTerminated()).isFalse();
    Assertions.assertThat(processor.peek()).isNull();
  }

  @Test
  public void shouldExpireValueOnDispose() {
    final TestPublisher<String> publisher = TestPublisher.create();
    // given
    final int minBackoff = 1;
    final int maxBackoff = 5;
    final int timeout = 10;

    final ReconnectMono<String> reconnectMono =
        publisher.mono().as(source -> new ReconnectMono<>(source, onExpire(), onValue()));

    StepVerifier.create(reconnectMono)
        .expectSubscription()
        .then(() -> publisher.next("value"))
        .expectNext("value")
        .expectComplete()
        .verify(Duration.ofSeconds(timeout));

    Assertions.assertThat(expired).isEmpty();
    Assertions.assertThat(received).hasSize(1);

    reconnectMono.dispose();

    Assertions.assertThat(expired).hasSize(1);
    Assertions.assertThat(received).hasSize(1);
    Assertions.assertThat(reconnectMono.isDisposed()).isTrue();

    StepVerifier.create(reconnectMono.subscribeOn(Schedulers.elastic()))
        .expectSubscription()
        .expectError(CancellationException.class)
        .verify(Duration.ofSeconds(timeout));
  }

  @Test
  public void shouldNotifyAllTheSubscribers() {
    final TestPublisher<String> publisher = TestPublisher.create();
    // given
    final int minBackoff = 1;
    final int maxBackoff = 5;
    final int timeout = 10;

    final ReconnectMono<String> reconnectMono =
        publisher.mono().as(source -> new ReconnectMono<>(source, onExpire(), onValue()));

    final MonoProcessor<String> sub1 = MonoProcessor.create();
    final MonoProcessor<String> sub2 = MonoProcessor.create();
    final MonoProcessor<String> sub3 = MonoProcessor.create();
    final MonoProcessor<String> sub4 = MonoProcessor.create();

    reconnectMono.subscribe(sub1);
    reconnectMono.subscribe(sub2);
    reconnectMono.subscribe(sub3);
    reconnectMono.subscribe(sub4);

    Assertions.assertThat(reconnectMono.subscribers).hasSize(4);

    final ArrayList<MonoProcessor<String>> processors = new ArrayList<>(200);

    for (int i = 0; i < 100; i++) {
      final MonoProcessor<String> subA = MonoProcessor.create();
      final MonoProcessor<String> subB = MonoProcessor.create();
      processors.add(subA);
      processors.add(subB);
      RaceTestUtils.race(() -> reconnectMono.subscribe(subA), () -> reconnectMono.subscribe(subB));
    }

    Assertions.assertThat(reconnectMono.subscribers).hasSize(204);

    sub1.dispose();

    Assertions.assertThat(reconnectMono.subscribers).hasSize(203);

    publisher.next("value");

    Assertions.assertThatThrownBy(sub1::peek).isInstanceOf(CancellationException.class);
    Assertions.assertThat(sub2.peek()).isEqualTo("value");
    Assertions.assertThat(sub3.peek()).isEqualTo("value");
    Assertions.assertThat(sub4.peek()).isEqualTo("value");

    for (MonoProcessor<String> sub : processors) {
      Assertions.assertThat(sub.peek()).isEqualTo("value");
      Assertions.assertThat(sub.isTerminated()).isTrue();
    }

    Assertions.assertThat(publisher.subscribeCount()).isEqualTo(1);
  }

  @Test
  public void shouldExpireValueExactlyOnce() {
    for (int i = 0; i < 1000; i++) {
      final TestPublisher<String> cold = TestPublisher.createCold();
      cold.next("value");
      // given
      final int minBackoff = 1;
      final int maxBackoff = 5;
      final int timeout = 10;

      final ReconnectMono<String> reconnectMono =
          cold.mono().as(source -> new ReconnectMono<>(source, onExpire(), onValue()));

      StepVerifier.create(reconnectMono.subscribeOn(Schedulers.elastic()))
          .expectSubscription()
          .expectNext("value")
          .expectComplete()
          .verify(Duration.ofSeconds(timeout));

      Assertions.assertThat(expired).isEmpty();
      Assertions.assertThat(received).hasSize(1).containsOnly(Tuples.of("value", reconnectMono));
      RaceTestUtils.race(reconnectMono::invalidate, reconnectMono::invalidate);

      Assertions.assertThat(expired).hasSize(1).containsOnly("value");
      Assertions.assertThat(received).hasSize(1).containsOnly(Tuples.of("value", reconnectMono));

      StepVerifier.create(reconnectMono.subscribeOn(Schedulers.elastic()))
          .expectSubscription()
          .expectNext("value")
          .expectComplete()
          .verify(Duration.ofSeconds(timeout));

      Assertions.assertThat(expired).hasSize(1).containsOnly("value");
      Assertions.assertThat(received)
          .hasSize(2)
          .containsOnly(Tuples.of("value", reconnectMono), Tuples.of("value", reconnectMono));

      Assertions.assertThat(cold.subscribeCount()).isEqualTo(2);

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
                    .subscribeOn(Schedulers.elastic()))
        .expectSubscription()
        .thenAwait(Duration.ofSeconds(timeout))
        .expectError(TimeoutException.class)
        .verify(Duration.ofSeconds(timeout));

    Assertions.assertThat(received).isEmpty();
    Assertions.assertThat(expired).isEmpty();
  }

  @Test
  public void monoRetryNoBackoff() {
    Mono<?> mono =
        Mono.error(new IOException())
            .retryWhen(Retry.max(2).doAfterRetry(onRetry()))
            .as(m -> new ReconnectMono<>(m, onExpire(), onValue()));

    StepVerifier.create(mono).verifyErrorMatches(Exceptions::isRetryExhausted);
    assertRetries(IOException.class, IOException.class);

    Assertions.assertThat(received).isEmpty();
    Assertions.assertThat(expired).isEmpty();
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

    Assertions.assertThat(received).isEmpty();
    Assertions.assertThat(expired).isEmpty();
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

    Assertions.assertThat(received).isEmpty();
    Assertions.assertThat(expired).isEmpty();
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
    assertEquals(exceptions.length, retries.size());
    int index = 0;
    for (Iterator<Retry.RetrySignal> it = retries.iterator(); it.hasNext(); ) {
      Retry.RetrySignal retryContext = it.next();
      assertEquals(index, retryContext.totalRetries());
      assertEquals(exceptions[index], retryContext.failure().getClass());
      index++;
    }
  }

  static boolean isRetryExhausted(Throwable e, Class<? extends Throwable> cause) {
    return Exceptions.isRetryExhausted(e) && cause.isInstance(e.getCause());
  }
}
