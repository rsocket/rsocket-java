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

import io.rsocket.internal.subscriber.AssertSubscriber;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Operators;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;
import reactor.test.util.RaceTestUtils;

public class ResolvingOperatorTests {

  @Test
  public void shouldExpireValueOnRacingDisposeAndComplete() {
    for (int i = 0; i < 10000; i++) {
      final int index = i;

      AssertSubscriber<String> subscriber = AssertSubscriber.create();
      subscriber.onSubscribe(Operators.emptySubscription());
      BiConsumer<String, Throwable> consumer =
          (v, t) -> {
            if (t != null) {
              subscriber.onError(t);
              return;
            }

            subscriber.onNext(v);
            subscriber.onComplete();
          };

      ResolvingTest.<String>create()
          .assertNothingExpired()
          .assertNothingReceived()
          .assertPendingResolution()
          .thenAddObserver(consumer)
          .assertPendingSubscribers(1)
          .assertPendingResolution()
          .then(self -> RaceTestUtils.race(() -> self.complete("value" + index), self::dispose))
          .assertDisposeCalled(1)
          .assertExpiredExactly("value" + index)
          .ifResolvedAssertEqual("value" + index)
          .assertIsDisposed();

      subscriber.assertTerminated();

      if (!subscriber.errors().isEmpty()) {
        Assertions.assertThat(subscriber.errors().get(0))
            .isInstanceOf(CancellationException.class)
            .hasMessage("Disposed");

      } else {
        Assertions.assertThat(subscriber.values()).containsExactly("value" + i);
      }
    }
  }

  @Test
  public void shouldNotifyAllTheSubscribersUnderRacingBetweenSubscribeAndComplete() {
    for (int i = 0; i < 10000; i++) {
      final String valueToSend = "value" + i;

      AssertSubscriber<String> subscriber = AssertSubscriber.create();
      subscriber.onSubscribe(Operators.emptySubscription());
      BiConsumer<String, Throwable> consumer =
          (v, t) -> {
            if (t != null) {
              subscriber.onError(t);
              return;
            }

            subscriber.onNext(v);
            subscriber.onComplete();
          };

      AssertSubscriber<String> subscriber2 = AssertSubscriber.create();
      subscriber2.onSubscribe(Operators.emptySubscription());
      BiConsumer<String, Throwable> consumer2 =
          (v, t) -> {
            if (t != null) {
              subscriber2.onError(t);
              return;
            }

            subscriber2.onNext(v);
            subscriber2.onComplete();
          };

      ResolvingTest.<String>create()
          .assertNothingExpired()
          .assertNothingReceived()
          .assertPendingSubscribers(0)
          .assertPendingResolution()
          .then(
              self -> {
                RaceTestUtils.race(() -> self.complete(valueToSend), () -> self.observe(consumer));

                subscriber.await(Duration.ofMillis(10)).assertValues(valueToSend).assertComplete();
              })
          .assertDisposeCalled(0)
          .assertReceivedExactly(valueToSend)
          .assertNothingExpired()
          .thenAddObserver(consumer2)
          .assertPendingSubscribers(0);

      subscriber2.await(Duration.ofMillis(10)).assertValues(valueToSend).assertComplete();
    }
  }

  @Test
  public void shouldNotExpireNewlyResolvedValueIfSubscribeIsRacingWithInvalidate() {
    for (int i = 0; i < 10000; i++) {
      final String valueToSend = "value" + i;
      final String valueToSend2 = "value2" + i;

      AssertSubscriber<String> subscriber = AssertSubscriber.create();
      subscriber.onSubscribe(Operators.emptySubscription());
      BiConsumer<String, Throwable> consumer =
          (v, t) -> {
            if (t != null) {
              subscriber.onError(t);
              return;
            }

            subscriber.onNext(v);
            subscriber.onComplete();
          };

      AssertSubscriber<String> subscriber2 = AssertSubscriber.create();
      subscriber2.onSubscribe(Operators.emptySubscription());
      BiConsumer<String, Throwable> consumer2 =
          (v, t) -> {
            if (t != null) {
              subscriber2.onError(t);
              return;
            }

            subscriber2.onNext(v);
            subscriber2.onComplete();
          };

      ResolvingTest.<String>create()
          .assertNothingExpired()
          .assertNothingReceived()
          .assertPendingSubscribers(0)
          .assertPendingResolution()
          .thenAddObserver(consumer)
          .then(
              self -> {
                self.complete(valueToSend);

                subscriber.await(Duration.ofMillis(10)).assertValues(valueToSend).assertComplete();
              })
          .assertReceivedExactly(valueToSend)
          .then(
              self ->
                  RaceTestUtils.race(
                      self::invalidate,
                      () -> {
                        self.observe(consumer2);
                        if (!subscriber2.isTerminated()) {
                          self.complete(valueToSend2);
                        }
                      }))
          .then(
              self -> {
                if (self.isPending()) {
                  self.assertReceivedExactly(valueToSend);
                } else {
                  self.assertReceivedExactly(valueToSend, valueToSend2);
                }
              })
          .assertExpiredExactly(valueToSend)
          .assertPendingSubscribers(0)
          .assertDisposeCalled(0)
          .then(
              self ->
                  subscriber2
                      .await(Duration.ofMillis(100))
                      .assertValueCount(1)
                      .assertValuesWith(
                          v -> {
                            if (self.subscribers == ResolvingOperator.READY) {
                              Assertions.assertThat(v).isEqualTo(valueToSend2);
                            } else {
                              Assertions.assertThat(v).isEqualTo(valueToSend);
                            }
                          })
                      .assertComplete());
    }
  }

  @Test
  public void shouldNotExpireNewlyResolvedValueIfSubscribeIsRacingWithInvalidates() {
    for (int i = 0; i < 10000; i++) {
      final String valueToSend = "value" + i;
      final String valueToSend2 = "value_to_possibly_expire" + i;

      AssertSubscriber<String> subscriber = AssertSubscriber.create();
      subscriber.onSubscribe(Operators.emptySubscription());
      BiConsumer<String, Throwable> consumer =
          (v, t) -> {
            if (t != null) {
              subscriber.onError(t);
              return;
            }

            subscriber.onNext(v);
            subscriber.onComplete();
          };

      AssertSubscriber<String> subscriber2 = AssertSubscriber.create();
      subscriber2.onSubscribe(Operators.emptySubscription());
      BiConsumer<String, Throwable> consumer2 =
          (v, t) -> {
            if (t != null) {
              subscriber2.onError(t);
              return;
            }

            subscriber2.onNext(v);
            subscriber2.onComplete();
          };

      ResolvingTest.<String>create()
          .assertNothingExpired()
          .assertNothingReceived()
          .assertPendingSubscribers(0)
          .assertPendingResolution()
          .thenAddObserver(consumer)
          .then(
              self -> {
                self.complete(valueToSend);

                subscriber.await(Duration.ofMillis(100)).assertValues(valueToSend).assertComplete();
              })
          .assertReceivedExactly(valueToSend)
          .then(
              self ->
                  RaceTestUtils.race(
                      self::invalidate,
                      self::invalidate,
                      () -> {
                        self.observe(consumer2);
                        if (!subscriber2.isTerminated()) {
                          self.complete(valueToSend2);
                        }
                      }))
          .then(
              self -> {
                if (!self.isPending()) {
                  self.assertReceivedExactly(valueToSend, valueToSend2);
                } else {
                  if (self.received.size() > 1) {
                    self.assertReceivedExactly(valueToSend, valueToSend2);
                  } else {
                    self.assertReceivedExactly(valueToSend);
                  }
                }

                Assertions.assertThat(self.expired)
                    .haveAtMost(
                        2,
                        new Condition<>(
                            new Predicate<Object>() {
                              int time = 0;

                              @Override
                              public boolean test(Object s) {
                                if (time++ == 0) {
                                  return valueToSend.equals(s);
                                } else {
                                  return valueToSend2.equals(s);
                                }
                              }
                            },
                            "should matches one of the given values"));
              })
          .assertPendingSubscribers(0)
          .assertDisposeCalled(0)
          .then(
              self ->
                  subscriber2
                      .await(Duration.ofMillis(100))
                      .assertValueCount(1)
                      .assertValuesWith(
                          v -> {
                            if (self.subscribers == ResolvingOperator.READY) {
                              Assertions.assertThat(v).isEqualTo(valueToSend2);
                            } else {
                              Assertions.assertThat(v).isIn(valueToSend, valueToSend2);
                            }
                          })
                      .assertComplete());
    }
  }

  @Test
  public void shouldNotExpireNewlyResolvedValueIfBlockIsRacingWithInvalidate() {
    for (int i = 0; i < 10000; i++) {
      final String valueToSend = "value" + i;
      final String valueToSend2 = "value2" + i;

      AssertSubscriber<String> subscriber = AssertSubscriber.create();
      subscriber.onSubscribe(Operators.emptySubscription());
      BiConsumer<String, Throwable> consumer =
          (v, t) -> {
            if (t != null) {
              subscriber.onError(t);
              return;
            }

            subscriber.onNext(v);
            subscriber.onComplete();
          };

      ResolvingTest.<String>create()
          .assertNothingExpired()
          .assertNothingReceived()
          .assertPendingSubscribers(0)
          .assertPendingResolution()
          .thenAddObserver(consumer)
          .then(
              self -> {
                self.complete(valueToSend);

                subscriber.await(Duration.ofMillis(10)).assertValues(valueToSend).assertComplete();
              })
          .assertReceivedExactly(valueToSend)
          .then(
              self ->
                  RaceTestUtils.race(
                      () ->
                          Assertions.assertThat(self.block(null))
                              .matches((v) -> v.equals(valueToSend) || v.equals(valueToSend2)),
                      self::invalidate,
                      () -> {
                        for (; ; ) {
                          if (self.subscribers != ResolvingOperator.READY) {
                            self.complete(valueToSend2);
                            break;
                          }
                        }
                      }))
          .then(
              self -> {
                if (self.isPending()) {
                  self.assertReceivedExactly(valueToSend);
                } else {
                  self.assertReceivedExactly(valueToSend, valueToSend2);
                }
              })
          .assertExpiredExactly(valueToSend)
          .assertPendingSubscribers(0)
          .assertDisposeCalled(0);
    }
  }

  @Test
  public void shouldEstablishValueOnceInCaseOfRacingBetweenSubscribers() {
    for (int i = 0; i < 10000; i++) {
      final String valueToSend = "value" + i;

      AssertSubscriber<String> subscriber = AssertSubscriber.create();
      subscriber.onSubscribe(Operators.emptySubscription());
      BiConsumer<String, Throwable> consumer =
          (v, t) -> {
            if (t != null) {
              subscriber.onError(t);
              return;
            }

            subscriber.onNext(v);
            subscriber.onComplete();
          };

      AssertSubscriber<String> subscriber2 = AssertSubscriber.create();
      subscriber2.onSubscribe(Operators.emptySubscription());
      BiConsumer<String, Throwable> consumer2 =
          (v, t) -> {
            if (t != null) {
              subscriber2.onError(t);
              return;
            }

            subscriber2.onNext(v);
            subscriber2.onComplete();
          };

      ResolvingTest.<String>create()
          .assertNothingExpired()
          .assertNothingReceived()
          .assertPendingSubscribers(0)
          .assertPendingResolution()
          .then(
              self ->
                  RaceTestUtils.race(() -> self.observe(consumer), () -> self.observe(consumer2)))
          .assertSubscribeCalled(1)
          .assertPendingSubscribers(2)
          .then(self -> self.complete(valueToSend))
          .assertPendingSubscribers(0)
          .assertReceivedExactly(valueToSend)
          .assertNothingExpired()
          .assertDisposeCalled(0)
          .then(
              self -> {
                Assertions.assertThat(subscriber.isTerminated()).isTrue();
                Assertions.assertThat(subscriber2.isTerminated()).isTrue();

                Assertions.assertThat(subscriber.values()).containsExactly(valueToSend);
                Assertions.assertThat(subscriber2.values()).containsExactly(valueToSend);

                Assertions.assertThat(self.subscribers).isEqualTo(ResolvingOperator.READY);

                Assertions.assertThat(self.add(consumer)).isEqualTo(ResolvingOperator.READY_STATE);
              });
    }
  }

  @Test
  public void shouldEstablishValueOnceInCaseOfRacingBetweenSubscribeAndBlock() {
    for (int i = 0; i < 10000; i++) {
      final String valueToSend = "value" + i;

      AssertSubscriber<String> subscriber = AssertSubscriber.create();
      subscriber.onSubscribe(Operators.emptySubscription());

      AssertSubscriber<String> subscriber2 = AssertSubscriber.create();
      subscriber2.onSubscribe(Operators.emptySubscription());
      BiConsumer<String, Throwable> consumer2 =
          (v, t) -> {
            if (t != null) {
              subscriber2.onError(t);
              return;
            }

            subscriber2.onNext(v);
            subscriber2.onComplete();
          };

      ResolvingTest.<String>create()
          .assertNothingExpired()
          .assertNothingReceived()
          .assertPendingSubscribers(0)
          .assertPendingResolution()
          .whenSubscribe(self -> self.complete(valueToSend))
          .then(
              self ->
                  RaceTestUtils.race(
                      () -> {
                        subscriber.onNext(self.block(null));
                        subscriber.onComplete();
                      },
                      () -> self.observe(consumer2)))
          .assertSubscribeCalled(1)
          .assertPendingSubscribers(0)
          .assertReceivedExactly(valueToSend)
          .assertNothingExpired()
          .assertDisposeCalled(0)
          .then(
              self -> {
                Assertions.assertThat(subscriber.isTerminated()).isTrue();
                Assertions.assertThat(subscriber2.isTerminated()).isTrue();

                Assertions.assertThat(subscriber.values()).containsExactly(valueToSend);
                Assertions.assertThat(subscriber2.values()).containsExactly(valueToSend);

                Assertions.assertThat(self.subscribers).isEqualTo(ResolvingOperator.READY);

                Assertions.assertThat(self.add(consumer2)).isEqualTo(ResolvingOperator.READY_STATE);
              });
    }
  }

  @Test
  public void shouldEstablishValueOnceInCaseOfRacingBetweenBlocks() {
    Duration timeout = Duration.ofMillis(100);
    for (int i = 0; i < 10000; i++) {
      final String valueToSend = "value" + i;

      AssertSubscriber<String> subscriber = AssertSubscriber.create();
      subscriber.onSubscribe(Operators.emptySubscription());

      AssertSubscriber<String> subscriber2 = AssertSubscriber.create();
      subscriber2.onSubscribe(Operators.emptySubscription());

      ResolvingTest.<String>create()
          .assertNothingExpired()
          .assertNothingReceived()
          .assertPendingSubscribers(0)
          .assertPendingResolution()
          .whenSubscribe(self -> self.complete(valueToSend))
          .then(
              self ->
                  RaceTestUtils.race(
                      () -> {
                        subscriber.onNext(self.block(timeout));
                        subscriber.onComplete();
                      },
                      () -> {
                        subscriber2.onNext(self.block(timeout));
                        subscriber2.onComplete();
                      }))
          .assertSubscribeCalled(1)
          .assertPendingSubscribers(0)
          .assertReceivedExactly(valueToSend)
          .assertNothingExpired()
          .assertDisposeCalled(0)
          .then(
              self -> {
                Assertions.assertThat(subscriber.isTerminated()).isTrue();
                Assertions.assertThat(subscriber2.isTerminated()).isTrue();

                Assertions.assertThat(subscriber.values()).containsExactly(valueToSend);
                Assertions.assertThat(subscriber2.values()).containsExactly(valueToSend);

                Assertions.assertThat(self.subscribers).isEqualTo(ResolvingOperator.READY);

                Assertions.assertThat(self.add((v, t) -> {}))
                    .isEqualTo(ResolvingOperator.READY_STATE);
              });
    }
  }

  @Test
  public void shouldExpireValueOnRacingDisposeAndError() {
    Hooks.onErrorDropped(t -> {});
    RuntimeException runtimeException = new RuntimeException("test");
    for (int i = 0; i < 10000; i++) {
      AssertSubscriber<String> subscriber = AssertSubscriber.create();
      subscriber.onSubscribe(Operators.emptySubscription());
      BiConsumer<String, Throwable> consumer =
          (v, t) -> {
            if (t != null) {
              subscriber.onError(t);
              return;
            }

            subscriber.onNext(v);
            subscriber.onComplete();
          };

      AssertSubscriber<String> subscriber2 = AssertSubscriber.create();
      subscriber2.onSubscribe(Operators.emptySubscription());
      BiConsumer<String, Throwable> consumer2 =
          (v, t) -> {
            if (t != null) {
              subscriber2.onError(t);
              return;
            }

            subscriber2.onNext(v);
            subscriber2.onComplete();
          };

      ResolvingTest.<String>create()
          .assertNothingExpired()
          .assertNothingReceived()
          .assertPendingSubscribers(0)
          .assertPendingResolution()
          .thenAddObserver(consumer)
          .assertSubscribeCalled(1)
          .assertPendingSubscribers(1)
          .then(self -> RaceTestUtils.race(() -> self.terminate(runtimeException), self::dispose))
          .assertPendingSubscribers(0)
          .assertNothingExpired()
          .assertDisposeCalled(1)
          .then(
              self -> {
                Assertions.assertThat(self.subscribers).isEqualTo(ResolvingOperator.TERMINATED);

                Assertions.assertThat(self.add((v, t) -> {}))
                    .isEqualTo(ResolvingOperator.TERMINATED_STATE);
              })
          .thenAddObserver(consumer2);

      subscriber
          .await(Duration.ofMillis(10))
          .assertErrorWith(
              t -> {
                if (t instanceof CancellationException) {
                  Assertions.assertThat(t)
                      .isInstanceOf(CancellationException.class)
                      .hasMessage("Disposed");
                } else {
                  Assertions.assertThat(t).isInstanceOf(RuntimeException.class).hasMessage("test");
                }
              });

      subscriber2
          .await(Duration.ofMillis(10))
          .assertErrorWith(
              t -> {
                if (t instanceof CancellationException) {
                  Assertions.assertThat(t)
                      .isInstanceOf(CancellationException.class)
                      .hasMessage("Disposed");
                } else {
                  Assertions.assertThat(t).isInstanceOf(RuntimeException.class).hasMessage("test");
                }
              });

      // no way to guarantee equality because of racing
      //      Assertions.assertThat(processor.getError())
      //                .isEqualTo(processor2.getError());
    }
  }

  @Test
  public void shouldThrowOnBlocking() {
    ResolvingTest.<String>create()
        .assertNothingExpired()
        .assertNothingReceived()
        .assertPendingSubscribers(0)
        .assertPendingResolution()
        .then(
            self ->
                Assertions.assertThatThrownBy(() -> self.block(Duration.ofMillis(100)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Timeout on Mono blocking read"))
        .assertPendingSubscribers(0)
        .assertNothingExpired()
        .assertNothingReceived()
        .assertDisposeCalled(0);
  }

  @Test
  public void shouldThrowOnBlockingIfHasAlreadyTerminated() {
    ResolvingTest.<String>create()
        .assertNothingExpired()
        .assertNothingReceived()
        .assertPendingSubscribers(0)
        .assertPendingResolution()
        .whenSubscribe(self -> self.terminate(new RuntimeException("test")))
        .then(
            self ->
                Assertions.assertThatThrownBy(() -> self.block(Duration.ofMillis(100)))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessage("test")
                    .hasSuppressedException(new Exception("Terminated with an error")))
        .assertPendingSubscribers(0)
        .assertNothingExpired()
        .assertNothingReceived()
        .assertDisposeCalled(1);
  }

  static Stream<Function<ResolvingTest<String>, Publisher<String>>> innerCases() {
    return Stream.of(
        (self) -> {
          final Sinks.One<String> processor = Sinks.unsafe().one();
          final ResolvingOperator.DeferredResolution<String, String> operator =
              new ResolvingOperator.DeferredResolution<String, String>(
                  self, new SinkOneSubscriber(processor)) {
                @Override
                public void accept(String v, Throwable t) {
                  if (t != null) {
                    onError(t);
                    return;
                  }

                  onNext(v);
                }
              };
          return processor
              .asMono()
              .doOnSubscribe(s -> self.observe(operator))
              .doOnCancel(operator::cancel);
        },
        (self) -> {
          final Sinks.One<String> processor = Sinks.unsafe().one();
          final SinkOneSubscriber subscriber = new SinkOneSubscriber(processor);
          final ResolvingOperator.MonoDeferredResolutionOperator<String> operator =
              new ResolvingOperator.MonoDeferredResolutionOperator<>(self, subscriber);
          subscriber.onSubscribe(operator);
          return processor
              .asMono()
              .doOnSubscribe(s -> self.observe(operator))
              .doOnCancel(operator::cancel);
        });
  }

  @ParameterizedTest
  @MethodSource("innerCases")
  public void shouldBePossibleToRemoveThemSelvesFromTheList_CancellationTest(
      Function<ResolvingTest<String>, Publisher<String>> caseProducer) {
    ResolvingTest.<String>create()
        .then(
            self -> {
              Publisher<String> resolvingInner = caseProducer.apply(self);
              StepVerifier.create(resolvingInner)
                  .expectSubscription()
                  .then(() -> self.assertSubscribeCalled(1).assertPendingSubscribers(1))
                  .thenCancel()
                  .verify(Duration.ofMillis(100));
            })
        .assertPendingSubscribers(0)
        .assertNothingExpired()
        .then(self -> self.complete("test"))
        .assertReceivedExactly("test");
  }

  @ParameterizedTest
  @MethodSource("innerCases")
  public void shouldExpireValueOnDispose(
      Function<ResolvingTest<String>, Publisher<String>> caseProducer) {
    ResolvingTest.<String>create()
        .then(
            self -> {
              Publisher<String> resolvingInner = caseProducer.apply(self);

              StepVerifier.create(resolvingInner)
                  .expectSubscription()
                  .then(() -> self.complete("test"))
                  .expectNext("test")
                  .expectComplete()
                  .verify(Duration.ofMillis(100));
            })
        .assertPendingSubscribers(0)
        .assertNothingExpired()
        .assertReceivedExactly("test")
        .then(ResolvingOperator::dispose)
        .assertExpiredExactly("test")
        .assertDisposeCalled(1);
  }

  @ParameterizedTest
  @MethodSource("innerCases")
  public void shouldNotifyAllTheSubscribers(
      Function<ResolvingTest<String>, Publisher<String>> caseProducer) {

    AssertSubscriber<String> sub1 = AssertSubscriber.create();
    AssertSubscriber<String> sub2 = AssertSubscriber.create();
    AssertSubscriber<String> sub3 = AssertSubscriber.create();
    AssertSubscriber<String> sub4 = AssertSubscriber.create();

    final ArrayList<AssertSubscriber<String>> processors = new ArrayList<>(200);

    ResolvingTest.<String>create()
        .assertDisposeCalled(0)
        .assertPendingSubscribers(0)
        .assertNothingExpired()
        .assertNothingReceived()
        .assertPendingResolution()
        .then(
            self -> {
              caseProducer.apply(self).subscribe(sub1);
              caseProducer.apply(self).subscribe(sub2);
              caseProducer.apply(self).subscribe(sub3);
              caseProducer.apply(self).subscribe(sub4);
            })
        .assertSubscribeCalled(1)
        .assertPendingSubscribers(4)
        .then(
            self -> {
              for (int i = 0; i < 100; i++) {
                AssertSubscriber<String> subA = AssertSubscriber.create();
                AssertSubscriber<String> subB = AssertSubscriber.create();
                processors.add(subA);
                processors.add(subB);
                RaceTestUtils.race(
                    () -> caseProducer.apply(self).subscribe(subA),
                    () -> caseProducer.apply(self).subscribe(subB));
              }
            })
        .assertSubscribeCalled(1)
        .assertPendingSubscribers(204)
        .then(self -> sub1.cancel())
        .assertPendingSubscribers(203)
        .then(
            self -> {
              String valueToSend = "value";
              self.complete(valueToSend);

              Assertions.assertThat(sub1.isTerminated()).isFalse();
              Assertions.assertThat(sub2.values()).containsExactly(valueToSend);
              Assertions.assertThat(sub3.values()).containsExactly(valueToSend);
              Assertions.assertThat(sub4.values()).containsExactly(valueToSend);

              for (AssertSubscriber<String> sub : processors) {
                Assertions.assertThat(sub.values()).containsExactly(valueToSend);
                Assertions.assertThat(sub.isTerminated()).isTrue();
              }
            })
        .assertPendingSubscribers(0)
        .assertNothingExpired()
        .assertReceivedExactly("value");
  }

  @Test
  public void shouldBeSerialIfRacyMonoInner() {
    for (int i = 0; i < 10000; i++) {
      long[] requested = new long[] {0};
      Subscription mockSubscription = Mockito.mock(Subscription.class);
      Mockito.doAnswer(
              a -> {
                long argument = a.getArgument(0);
                return requested[0] += argument;
              })
          .when(mockSubscription)
          .request(Mockito.anyLong());
      ResolvingOperator.DeferredResolution resolution =
          new ResolvingOperator.DeferredResolution(
              ResolvingTest.create(), AssertSubscriber.create(0)) {

            @Override
            public void accept(Object o, Object o2) {}
          };

      resolution.request(5);

      RaceTestUtils.race(
          () -> resolution.onSubscribe(mockSubscription),
          () -> {
            resolution.request(10);
            resolution.request(10);
            resolution.request(10);
          });

      resolution.request(15);

      Assertions.assertThat(requested[0]).isEqualTo(50L);
    }
  }

  @Test
  public void shouldExpireValueExactlyOnceOnRacingBetweenInvalidates() {
    for (int i = 0; i < 10000; i++) {
      ResolvingTest.create()
          .assertNothingExpired()
          .assertNothingReceived()
          .assertPendingResolution()
          .then(self -> self.complete("test"))
          .assertReceivedExactly("test")
          .then(self -> RaceTestUtils.race(self::invalidate, self::invalidate))
          .assertExpiredExactly("test");
    }
  }

  @Test
  public void shouldExpireValueExactlyOnceOnRacingBetweenInvalidateAndDispose() {
    for (int i = 0; i < 10000; i++) {
      ResolvingTest.create()
          .assertNothingExpired()
          .assertNothingReceived()
          .assertPendingResolution()
          .then(self -> self.complete("test"))
          .assertReceivedExactly("test")
          .then(self -> RaceTestUtils.race(self::invalidate, self::dispose))
          .assertExpiredExactly("test");
    }
  }

  static class ResolvingTest<T> extends ResolvingOperator<T> {

    final AtomicInteger subscribeCalls = new AtomicInteger();
    final AtomicInteger onDisposeCalls = new AtomicInteger();

    final Queue<Object> received = new ConcurrentLinkedQueue<>();
    final Queue<Object> expired = new ConcurrentLinkedQueue<>();

    Consumer<ResolvingTest<T>> whenSubscribeConsumer = (self) -> {};

    static <T> ResolvingTest<T> create() {
      return new ResolvingTest<>();
    }

    public ResolvingTest<T> assertPendingSubscribers(int cnt) {
      Assertions.assertThat(this.subscribers.length).isEqualTo(cnt);

      return this;
    }

    public ResolvingTest<T> whenSubscribe(Consumer<ResolvingTest<T>> consumer) {
      this.whenSubscribeConsumer = consumer;
      return this;
    }

    public ResolvingTest<T> then(Consumer<ResolvingTest<T>> consumer) {
      consumer.accept(this);

      return this;
    }

    public ResolvingTest<T> thenAddObserver(BiConsumer<T, Throwable> consumer) {
      this.observe(consumer);
      return this;
    }

    public ResolvingTest<T> assertPendingResolution() {
      Assertions.assertThat(this.isPending()).isTrue();

      return this;
    }

    public ResolvingTest<T> assertIsDisposed() {
      Assertions.assertThat(this.isDisposed()).isTrue();

      return this;
    }

    public ResolvingTest<T> assertSubscribeCalled(int times) {
      Assertions.assertThat(subscribeCalls).hasValue(times);

      return this;
    }

    public ResolvingTest<T> assertDisposeCalled(int times) {
      Assertions.assertThat(onDisposeCalls).hasValue(times);
      return this;
    }

    public ResolvingTest<T> assertNothingExpired() {
      return assertExpiredExactly();
    }

    public ResolvingTest<T> assertExpiredExactly(T... values) {
      Assertions.assertThat(expired).hasSize(values.length).containsExactly(values);

      return this;
    }

    public ResolvingTest<T> assertNothingReceived() {
      return assertReceivedExactly();
    }

    public ResolvingTest<T> assertReceivedExactly(T... values) {
      Assertions.assertThat(received).hasSize(values.length).containsExactly(values);

      return this;
    }

    public ResolvingTest<T> ifResolvedAssertEqual(T value) {
      if (received.size() > 0) {
        Assertions.assertThat(received).hasSize(1).containsExactly(value);
      }

      return this;
    }

    @Override
    protected void doOnValueResolved(T value) {
      received.offer(value);
    }

    @Override
    protected void doOnValueExpired(T value) {
      expired.offer(value);
    }

    @Override
    protected void doSubscribe() {
      whenSubscribeConsumer.accept(this);
      subscribeCalls.incrementAndGet();
    }

    @Override
    protected void doOnDispose() {
      onDisposeCalls.incrementAndGet();
    }
  }

  private static class SinkOneSubscriber implements CoreSubscriber<String> {

    private final Sinks.One<String> processor;
    private boolean valueReceived;

    public SinkOneSubscriber(Sinks.One<String> processor) {
      this.processor = processor;
    }

    @Override
    public void onSubscribe(Subscription s) {
      s.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(String s) {
      valueReceived = true;
      processor.tryEmitValue(s);
    }

    @Override
    public void onError(Throwable t) {
      processor.tryEmitError(t);
    }

    @Override
    public void onComplete() {
      if (!valueReceived) {
        processor.tryEmitEmpty();
      }
    }
  }
}
