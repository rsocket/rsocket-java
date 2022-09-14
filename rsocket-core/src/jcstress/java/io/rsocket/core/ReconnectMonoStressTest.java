/*
 * Copyright 2015-Present the original author or authors.
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

import static io.rsocket.core.ResolvingOperator.EMPTY_SUBSCRIBED;
import static io.rsocket.core.ResolvingOperator.EMPTY_UNSUBSCRIBED;
import static io.rsocket.core.ResolvingOperator.READY;
import static io.rsocket.core.ResolvingOperator.TERMINATED;
import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.BiConsumer;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.IIIIIII_Result;
import org.openjdk.jcstress.infra.results.IIIIII_Result;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;

public abstract class ReconnectMonoStressTest {

  abstract static class BaseStressTest {

    final StressSubscription<String> stressSubscription = new StressSubscription<>();

    final Mono<String> source = source();

    final StressSubscriber<String> stressSubscriber = new StressSubscriber<>();

    volatile int onValueExpire;

    static final AtomicIntegerFieldUpdater<BaseStressTest> ON_VALUE_EXPIRE =
        AtomicIntegerFieldUpdater.newUpdater(BaseStressTest.class, "onValueExpire");

    volatile int onValueReceived;

    static final AtomicIntegerFieldUpdater<BaseStressTest> ON_VALUE_RECEIVED =
        AtomicIntegerFieldUpdater.newUpdater(BaseStressTest.class, "onValueReceived");
    final ReconnectMono<String> reconnectMono =
        new ReconnectMono<>(
            source,
            (__) -> ON_VALUE_EXPIRE.incrementAndGet(BaseStressTest.this),
            (__, ___) -> ON_VALUE_RECEIVED.incrementAndGet(BaseStressTest.this));

    abstract Mono<String> source();

    int state() {
      final BiConsumer<String, Throwable>[] subscribers = reconnectMono.resolvingInner.subscribers;
      if (subscribers == EMPTY_UNSUBSCRIBED) {
        return 0;
      } else if (subscribers == EMPTY_SUBSCRIBED) {
        return 1;
      } else if (subscribers == READY) {
        return 2;
      } else if (subscribers == TERMINATED) {
        return 3;
      } else {
        return 4;
      }
    }
  }

  @JCStressTest
  @Outcome(
      id = {"1, 0, 0, 1, 1, 0, 3"},
      expect = ACCEPTABLE,
      desc = "Disposed before value is delivered")
  @Outcome(
      id = {"0, 0, 0, 1, 1, 0, 3"},
      expect = ACCEPTABLE,
      desc = "Disposed after onComplete but before value is delivered")
  @Outcome(
      id = {"0, 1, 1, 0, 1, 1, 3"},
      expect = ACCEPTABLE,
      desc = "Disposed after value is delivered")
  @State
  public static class ExpireValueOnRacingDisposeAndNext extends BaseStressTest {

    {
      reconnectMono.subscribe(stressSubscriber);
    }

    @Override
    Mono<String> source() {
      return new Mono<String>() {
        @Override
        public void subscribe(CoreSubscriber<? super String> actual) {
          stressSubscription.subscribe(actual);
        }
      };
    }

    @Actor
    void sendNext() {
      stressSubscription.actual.onNext("value");
      stressSubscription.actual.onComplete();
    }

    @Actor
    void dispose() {
      reconnectMono.dispose();
    }

    @Arbiter
    public void arbiter(IIIIIII_Result r) {
      r.r1 = stressSubscription.cancelled ? 1 : 0;
      r.r2 = stressSubscriber.onNextCalls;
      r.r3 = stressSubscriber.onCompleteCalls;
      r.r4 = stressSubscriber.onErrorCalls;
      r.r5 = onValueExpire;
      r.r6 = onValueReceived;
      r.r7 = state();
    }
  }

  @JCStressTest
  @Outcome(
      id = {"1, 0, 0, 1, 1, 0, 3"},
      expect = ACCEPTABLE,
      desc = "Disposed before error is delivered")
  @Outcome(
      id = {"0, 0, 0, 1, 1, 0, 3"},
      expect = ACCEPTABLE,
      desc = "Disposed after onError")
  @State
  public static class ExpireValueOnRacingDisposeAndError extends BaseStressTest {

    {
      Hooks.onErrorDropped(t -> {});
      reconnectMono.subscribe(stressSubscriber);
    }

    @Override
    Mono<String> source() {
      return new Mono<String>() {
        @Override
        public void subscribe(CoreSubscriber<? super String> actual) {
          stressSubscription.subscribe(actual);
        }
      };
    }

    @Actor
    void sendNext() {
      stressSubscription.actual.onNext("value");
      stressSubscription.actual.onError(new RuntimeException("boom"));
    }

    @Actor
    void dispose() {
      reconnectMono.dispose();
    }

    @Arbiter
    public void arbiter(IIIIIII_Result r) {
      Hooks.resetOnErrorDropped();

      r.r1 = stressSubscription.cancelled ? 1 : 0;
      r.r2 = stressSubscriber.onNextCalls;
      r.r3 = stressSubscriber.onCompleteCalls;
      r.r4 = stressSubscriber.onErrorCalls;
      r.r5 = onValueExpire;
      r.r6 = onValueReceived;
      r.r7 = state();
    }
  }

  @JCStressTest
  @Outcome(
      id = {"0, 1, 1, 0, 0, 1, 2"},
      expect = ACCEPTABLE,
      desc = "Invalidate happens before value is delivered")
  @Outcome(
      id = {"0, 1, 1, 0, 1, 1, 0"},
      expect = ACCEPTABLE,
      desc = "Invalidate happens after value is delivered")
  @State
  public static class ExpireValueOnRacingInvalidateAndNextComplete extends BaseStressTest {

    {
      reconnectMono.subscribe(stressSubscriber);
    }

    @Override
    Mono<String> source() {
      return new Mono<String>() {
        @Override
        public void subscribe(CoreSubscriber<? super String> actual) {
          stressSubscription.subscribe(actual);
        }
      };
    }

    @Actor
    void sendNext() {
      stressSubscription.actual.onNext("value");
      stressSubscription.actual.onComplete();
    }

    @Actor
    void invalidate() {
      reconnectMono.invalidate();
    }

    @Arbiter
    public void arbiter(IIIIIII_Result r) {
      r.r1 = stressSubscription.cancelled ? 1 : 0;
      r.r2 = stressSubscriber.onNextCalls;
      r.r3 = stressSubscriber.onCompleteCalls;
      r.r4 = stressSubscriber.onErrorCalls;
      r.r5 = onValueExpire;
      r.r6 = onValueReceived;
      r.r7 = state();
    }
  }

  @JCStressTest
  @Outcome(
      id = {"0, 1, 1, 0, 1, 1, 0"},
      expect = ACCEPTABLE)
  @State
  public static class ExpireValueOnceOnRacingInvalidateAndInvalidate extends BaseStressTest {

    {
      reconnectMono.subscribe(stressSubscriber);
    }

    @Override
    Mono<String> source() {
      return new Mono<String>() {
        @Override
        public void subscribe(CoreSubscriber<? super String> actual) {
          stressSubscription.subscribe(actual);
          stressSubscription.actual.onNext("value");
          stressSubscription.actual.onComplete();
        }
      };
    }

    @Actor
    void invalidate1() {
      reconnectMono.invalidate();
    }

    @Actor
    void invalidate2() {
      reconnectMono.invalidate();
    }

    @Arbiter
    public void arbiter(IIIIIII_Result r) {
      r.r1 = stressSubscription.cancelled ? 1 : 0;
      r.r2 = stressSubscriber.onNextCalls;
      r.r3 = stressSubscriber.onCompleteCalls;
      r.r4 = stressSubscriber.onErrorCalls;
      r.r5 = onValueExpire;
      r.r6 = onValueReceived;
      r.r7 = state();
    }
  }

  @JCStressTest
  @Outcome(
      id = {"0, 1, 1, 0, 1, 1, 3"},
      expect = ACCEPTABLE)
  @State
  public static class ExpireValueOnceOnRacingInvalidateAndDispose extends BaseStressTest {

    {
      reconnectMono.subscribe(stressSubscriber);
    }

    @Override
    Mono<String> source() {
      return new Mono<String>() {
        @Override
        public void subscribe(CoreSubscriber<? super String> actual) {
          stressSubscription.subscribe(actual);
          stressSubscription.actual.onNext("value");
          stressSubscription.actual.onComplete();
        }
      };
    }

    @Actor
    void invalidate() {
      reconnectMono.invalidate();
    }

    @Actor
    void dispose() {
      reconnectMono.dispose();
    }

    @Arbiter
    public void arbiter(IIIIIII_Result r) {
      r.r1 = stressSubscription.cancelled ? 1 : 0;
      r.r2 = stressSubscriber.onNextCalls;
      r.r3 = stressSubscriber.onCompleteCalls;
      r.r4 = stressSubscriber.onErrorCalls;
      r.r5 = onValueExpire;
      r.r6 = onValueReceived;
      r.r7 = state();
    }
  }

  @JCStressTest
  @Outcome(
      id = {"1, 0, 2, 2, 0, 1"},
      expect = ACCEPTABLE)
  @State
  public static class DeliversValueToAllSubscribersUnderRace extends BaseStressTest {

    final StressSubscriber<String> stressSubscriber2 = new StressSubscriber<>();

    {
      reconnectMono.subscribe(stressSubscriber);
    }

    @Override
    Mono<String> source() {
      return new Mono<String>() {
        @Override
        public void subscribe(CoreSubscriber<? super String> actual) {
          stressSubscription.subscribe(actual);
        }
      };
    }

    @Actor
    void sendNextAndComplete() {
      stressSubscription.actual.onNext("value");
      stressSubscription.actual.onComplete();
    }

    @Actor
    void secondSubscribe() {
      reconnectMono.subscribe(stressSubscriber2);
    }

    @Arbiter
    public void arbiter(IIIIII_Result r) {
      r.r1 = stressSubscription.requestsCount;
      r.r2 = stressSubscription.cancelled ? 1 : 0;
      r.r3 = stressSubscriber.onNextCalls + stressSubscriber2.onNextCalls;
      r.r4 = stressSubscriber.onCompleteCalls + stressSubscriber2.onCompleteCalls;
      r.r5 = onValueExpire;
      r.r6 = onValueReceived;
    }
  }

  @JCStressTest
  @Outcome(
      id = {"2, 0, 1, 1, 1, 1, 4"},
      expect = ACCEPTABLE,
      desc = "Second Subscriber subscribed after invalidate")
  @Outcome(
      id = {"1, 0, 2, 2, 1, 1, 0"},
      expect = ACCEPTABLE,
      desc = "Second Subscriber subscribed before invalidate and received value")
  @State
  public static class InvalidateAndSubscribeUnderRace extends BaseStressTest {

    final StressSubscriber<String> stressSubscriber2 = new StressSubscriber<>();

    {
      reconnectMono.subscribe(stressSubscriber);
      stressSubscription.actual.onNext("value");
      stressSubscription.actual.onComplete();
    }

    @Override
    Mono<String> source() {
      return new Mono<String>() {
        @Override
        public void subscribe(CoreSubscriber<? super String> actual) {
          stressSubscription.subscribe(actual);
        }
      };
    }

    @Actor
    void invalidate() {
      reconnectMono.invalidate();
    }

    @Actor
    void secondSubscribe() {
      reconnectMono.subscribe(stressSubscriber2);
    }

    @Arbiter
    public void arbiter(IIIIIII_Result r) {
      r.r1 = stressSubscription.subscribes;
      r.r2 = stressSubscription.cancelled ? 1 : 0;
      r.r3 = stressSubscriber.onNextCalls + stressSubscriber2.onNextCalls;
      r.r4 = stressSubscriber.onCompleteCalls + stressSubscriber2.onCompleteCalls;
      r.r5 = onValueExpire;
      r.r6 = onValueReceived;
      r.r7 = state();
    }
  }

  @JCStressTest
  @Outcome(
      id = {"2, 0, 2, 1, 2, 2"},
      expect = ACCEPTABLE,
      desc = "Subscribed again after invalidate")
  @Outcome(
      id = {"1, 0, 1, 1, 1, 0"},
      expect = ACCEPTABLE,
      desc = "Subscribed before invalidate")
  @State
  public static class InvalidateAndBlockUnderRace extends BaseStressTest {

    String receivedValue;

    {
      reconnectMono.subscribe(stressSubscriber);
    }

    @Override
    Mono<String> source() {
      return new Mono<String>() {
        @Override
        public void subscribe(CoreSubscriber<? super String> actual) {
          stressSubscription.subscribe(actual);
          actual.onNext("value" + stressSubscription.subscribes);
          actual.onComplete();
        }
      };
    }

    @Actor
    void invalidate() {
      reconnectMono.invalidate();
    }

    @Actor
    void secondSubscribe() {
      receivedValue = reconnectMono.block();
    }

    @Arbiter
    public void arbiter(IIIIII_Result r) {
      r.r1 = stressSubscription.subscribes;
      r.r2 = stressSubscription.cancelled ? 1 : 0;
      r.r3 = receivedValue.equals("value1") ? 1 : receivedValue.equals("value2") ? 2 : -1;
      r.r4 = onValueExpire;
      r.r5 = onValueReceived;
      r.r6 = state();
    }
  }

  @JCStressTest
  @Outcome(
      id = {"1, 0, 1, 0, 1, 2"},
      expect = ACCEPTABLE)
  @State
  public static class TwoSubscribesRace extends BaseStressTest {

    StressSubscriber<String> stressSubscriber2 = new StressSubscriber<>();

    @Override
    Mono<String> source() {
      return new Mono<String>() {
        @Override
        public void subscribe(CoreSubscriber<? super String> actual) {
          stressSubscription.subscribe(actual);
          actual.onNext("value" + stressSubscription.subscribes);
          actual.onComplete();
        }
      };
    }

    @Actor
    void subscribe1() {
      reconnectMono.subscribe(stressSubscriber);
    }

    @Actor
    void subscribe2() {
      reconnectMono.subscribe(stressSubscriber2);
    }

    @Arbiter
    public void arbiter(IIIIII_Result r) {
      r.r1 = stressSubscription.subscribes;
      r.r2 = stressSubscription.cancelled ? 1 : 0;
      r.r3 = stressSubscriber.values.get(0).equals(stressSubscriber2.values.get(0)) ? 1 : 2;
      r.r4 = onValueExpire;
      r.r5 = onValueReceived;
      r.r6 = state();
    }
  }

  @JCStressTest
  @Outcome(
      id = {"1, 0, 1, 0, 1, 2"},
      expect = ACCEPTABLE)
  @State
  public static class SubscribeBlockConnectRace extends BaseStressTest {

    String receivedValue;

    @Override
    Mono<String> source() {
      return new Mono<String>() {
        @Override
        public void subscribe(CoreSubscriber<? super String> actual) {
          stressSubscription.subscribe(actual);
          actual.onNext("value" + stressSubscription.subscribes);
          actual.onComplete();
        }
      };
    }

    @Actor
    void block() {
      receivedValue = reconnectMono.block();
    }

    @Actor
    void subscribe() {
      reconnectMono.subscribe(stressSubscriber);
    }

    @Actor
    void connect() {
      reconnectMono.resolvingInner.connect();
    }

    @Arbiter
    public void arbiter(IIIIII_Result r) {
      r.r1 = stressSubscription.subscribes;
      r.r2 = stressSubscription.cancelled ? 1 : 0;
      r.r3 = receivedValue.equals(stressSubscriber.values.get(0)) ? 1 : 2;
      r.r4 = onValueExpire;
      r.r5 = onValueReceived;
      r.r6 = state();
    }
  }

  @JCStressTest
  @Outcome(
      id = {"1, 0, 1, 0, 1, 2"},
      expect = ACCEPTABLE)
  @State
  public static class TwoBlocksRace extends BaseStressTest {

    String receivedValue1;
    String receivedValue2;

    @Override
    Mono<String> source() {
      return new Mono<String>() {
        @Override
        public void subscribe(CoreSubscriber<? super String> actual) {
          stressSubscription.subscribe(actual);
          actual.onNext("value" + stressSubscription.subscribes);
          actual.onComplete();
        }
      };
    }

    @Actor
    void block1() {
      receivedValue1 = reconnectMono.block();
    }

    @Actor
    void block2() {
      receivedValue2 = reconnectMono.block();
    }

    @Arbiter
    public void arbiter(IIIIII_Result r) {
      r.r1 = stressSubscription.subscribes;
      r.r2 = stressSubscription.cancelled ? 1 : 0;
      r.r3 = receivedValue1.equals(receivedValue2) ? 1 : 2;
      r.r4 = onValueExpire;
      r.r5 = onValueReceived;
      r.r6 = state();
    }
  }
}
