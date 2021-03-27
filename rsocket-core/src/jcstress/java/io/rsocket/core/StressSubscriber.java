/*
 * Copyright (c) 2020-Present Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.rsocket.core;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Operators;
import reactor.util.context.Context;

public class StressSubscriber<T> implements CoreSubscriber<T> {

  enum Operation {
    ON_NEXT,
    ON_ERROR,
    ON_COMPLETE,
    ON_SUBSCRIBE
  }

  final long initRequest;

  final Context context;

  volatile Subscription subscription;

  @SuppressWarnings("rawtypes")
  static final AtomicReferenceFieldUpdater<StressSubscriber, Subscription> S =
      AtomicReferenceFieldUpdater.newUpdater(
          StressSubscriber.class, Subscription.class, "subscription");

  public Throwable error;

  public List<Throwable> droppedErrors = new CopyOnWriteArrayList<>();

  public List<T> values = new ArrayList<>();

  public volatile Operation guard;

  @SuppressWarnings("rawtypes")
  static final AtomicReferenceFieldUpdater<StressSubscriber, Operation> GUARD =
      AtomicReferenceFieldUpdater.newUpdater(StressSubscriber.class, Operation.class, "guard");

  public volatile boolean concurrentOnNext;

  public volatile boolean concurrentOnError;

  public volatile boolean concurrentOnComplete;

  public volatile boolean concurrentOnSubscribe;

  public volatile int onNextCalls;

  @SuppressWarnings("rawtypes")
  static final AtomicIntegerFieldUpdater<StressSubscriber> ON_NEXT_CALLS =
      AtomicIntegerFieldUpdater.newUpdater(StressSubscriber.class, "onNextCalls");

  public volatile int onNextDiscarded;

  @SuppressWarnings("rawtypes")
  static final AtomicIntegerFieldUpdater<StressSubscriber> ON_NEXT_DISCARDED =
      AtomicIntegerFieldUpdater.newUpdater(StressSubscriber.class, "onNextDiscarded");

  public volatile int onErrorCalls;

  @SuppressWarnings("rawtypes")
  static final AtomicIntegerFieldUpdater<StressSubscriber> ON_ERROR_CALLS =
      AtomicIntegerFieldUpdater.newUpdater(StressSubscriber.class, "onErrorCalls");

  public volatile int onCompleteCalls;

  @SuppressWarnings("rawtypes")
  static final AtomicIntegerFieldUpdater<StressSubscriber> ON_COMPLETE_CALLS =
      AtomicIntegerFieldUpdater.newUpdater(StressSubscriber.class, "onCompleteCalls");

  public volatile int onSubscribeCalls;

  @SuppressWarnings("rawtypes")
  static final AtomicIntegerFieldUpdater<StressSubscriber> ON_SUBSCRIBE_CALLS =
      AtomicIntegerFieldUpdater.newUpdater(StressSubscriber.class, "onSubscribeCalls");

  /** Build a {@link StressSubscriber} that makes an unbounded request upon subscription. */
  public StressSubscriber() {
    this(Long.MAX_VALUE);
  }

  /**
   * Build a {@link StressSubscriber} that requests the provided amount in {@link
   * #onSubscribe(Subscription)}. Use {@code 0} to avoid any initial request upon subscription.
   *
   * @param initRequest the requested amount upon subscription, or zero to disable initial request
   */
  public StressSubscriber(long initRequest) {
    this.initRequest = initRequest;
    this.context =
        Operators.enableOnDiscard(
            Context.of(
                "reactor.onErrorDropped.local",
                (Consumer<Throwable>)
                    throwable -> {
                      droppedErrors.add(throwable);
                    }),
            (__) -> ON_NEXT_DISCARDED.incrementAndGet(this));
  }

  @Override
  public Context currentContext() {
    return this.context;
  }

  @Override
  public void onSubscribe(Subscription subscription) {
    if (!GUARD.compareAndSet(this, null, Operation.ON_SUBSCRIBE)) {
      concurrentOnSubscribe = true;
    } else {
      boolean wasSet = Operators.setOnce(S, this, subscription);
      GUARD.compareAndSet(this, Operation.ON_SUBSCRIBE, null);
      if (wasSet) {
        if (initRequest > 0) {
          subscription.request(initRequest);
        }
      }
    }
    ON_SUBSCRIBE_CALLS.incrementAndGet(this);
  }

  @Override
  public void onNext(T value) {
    if (!GUARD.compareAndSet(this, null, Operation.ON_NEXT)) {
      concurrentOnNext = true;
    } else {
      values.add(value);
      GUARD.compareAndSet(this, Operation.ON_NEXT, null);
    }
    ON_NEXT_CALLS.incrementAndGet(this);
  }

  @Override
  public void onError(Throwable throwable) {
    if (!GUARD.compareAndSet(this, null, Operation.ON_ERROR)) {
      concurrentOnError = true;
    } else {
      GUARD.compareAndSet(this, Operation.ON_ERROR, null);
    }
    error = throwable;
    ON_ERROR_CALLS.incrementAndGet(this);
  }

  @Override
  public void onComplete() {
    if (!GUARD.compareAndSet(this, null, Operation.ON_COMPLETE)) {
      concurrentOnComplete = true;
    } else {
      GUARD.compareAndSet(this, Operation.ON_COMPLETE, null);
    }
    ON_COMPLETE_CALLS.incrementAndGet(this);
  }

  public void request(long n) {
    if (Operators.validate(n)) {
      Subscription s = this.subscription;
      if (s != null) {
        s.request(n);
      }
    }
  }

  public void cancel() {
    Operators.terminate(S, this);
  }
}
