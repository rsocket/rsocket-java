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

import static reactor.core.publisher.Operators.addCap;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.publisher.Operators;
import reactor.util.context.Context;

public class StressSubscriber<T> implements CoreSubscriber<T> {

  enum Operation {
    ON_NEXT,
    ON_ERROR,
    ON_COMPLETE,
    ON_SUBSCRIBE
  }

  final Context context;
  final int requestedFusionMode;

  int fusionMode;
  Subscription subscription;

  public Throwable error;
  public boolean done;

  public List<Throwable> droppedErrors = new CopyOnWriteArrayList<>();

  public List<T> values = new ArrayList<>();

  volatile long requested;

  @SuppressWarnings("rawtypes")
  static final AtomicLongFieldUpdater<StressSubscriber> REQUESTED =
      AtomicLongFieldUpdater.newUpdater(StressSubscriber.class, "requested");

  volatile int wip;

  @SuppressWarnings("rawtypes")
  static final AtomicIntegerFieldUpdater<StressSubscriber> WIP =
      AtomicIntegerFieldUpdater.newUpdater(StressSubscriber.class, "wip");

  public volatile Operation guard;

  @SuppressWarnings("rawtypes")
  static final AtomicReferenceFieldUpdater<StressSubscriber, Operation> GUARD =
      AtomicReferenceFieldUpdater.newUpdater(StressSubscriber.class, Operation.class, "guard");

  public volatile boolean concurrentOnNext;

  public volatile boolean concurrentOnError;

  public volatile boolean concurrentOnComplete;

  public volatile boolean concurrentOnSubscribe;

  public volatile int onNextCalls;

  public BlockingQueue<Throwable> q = new LinkedBlockingDeque<>();

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
    this(Long.MAX_VALUE, Fuseable.NONE);
  }

  /**
   * Build a {@link StressSubscriber} that requests the provided amount in {@link
   * #onSubscribe(Subscription)}. Use {@code 0} to avoid any initial request upon subscription.
   *
   * @param initRequest the requested amount upon subscription, or zero to disable initial request
   */
  public StressSubscriber(long initRequest) {
    this(initRequest, Fuseable.NONE);
  }

  /**
   * Build a {@link StressSubscriber} that requests the provided amount in {@link
   * #onSubscribe(Subscription)}. Use {@code 0} to avoid any initial request upon subscription.
   *
   * @param initRequest the requested amount upon subscription, or zero to disable initial request
   */
  public StressSubscriber(long initRequest, int requestedFusionMode) {
    this.requestedFusionMode = requestedFusionMode;
    this.context =
        Operators.enableOnDiscard(
            Context.of(
                "reactor.onErrorDropped.local",
                (Consumer<Throwable>) throwable -> droppedErrors.add(throwable)),
            (__) -> ON_NEXT_DISCARDED.incrementAndGet(this));
    REQUESTED.lazySet(this, initRequest | Long.MIN_VALUE);
  }

  @Override
  public Context currentContext() {
    return this.context;
  }

  @Override
  public void onSubscribe(Subscription subscription) {
    if (!GUARD.compareAndSet(this, null, Operation.ON_SUBSCRIBE)) {
      concurrentOnSubscribe = true;
      subscription.cancel();
    } else {
      final boolean isValid = Operators.validate(this.subscription, subscription);
      if (isValid) {
        this.subscription = subscription;
      }
      GUARD.compareAndSet(this, Operation.ON_SUBSCRIBE, null);

      if (this.requestedFusionMode > 0 && subscription instanceof Fuseable.QueueSubscription) {
        final int m =
            ((Fuseable.QueueSubscription<?>) subscription).requestFusion(this.requestedFusionMode);
        final long requested = this.requested;
        this.fusionMode = m;
        if (m != Fuseable.NONE) {
          if (requested == Long.MAX_VALUE) {
            subscription.cancel();
          }
          drain();
          return;
        }
      }

      if (isValid) {
        long delivered = 0;
        for (; ; ) {
          long s = requested;
          if (s == Long.MAX_VALUE) {
            subscription.cancel();
            break;
          }

          long r = s & Long.MAX_VALUE;
          long toRequest = r - delivered;
          if (toRequest > 0) {
            subscription.request(toRequest);
            delivered = r;
          }

          if (REQUESTED.compareAndSet(this, s, 0)) {
            break;
          }
        }
      }
    }
    ON_SUBSCRIBE_CALLS.incrementAndGet(this);
  }

  @Override
  public void onNext(T value) {
    if (fusionMode == Fuseable.ASYNC) {
      drain();
      return;
    }

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
    done = true;
    q.offer(throwable);
    ON_ERROR_CALLS.incrementAndGet(this);

    if (fusionMode == Fuseable.ASYNC) {
      drain();
    }
  }

  @Override
  public void onComplete() {
    if (!GUARD.compareAndSet(this, null, Operation.ON_COMPLETE)) {
      concurrentOnComplete = true;
    } else {
      GUARD.compareAndSet(this, Operation.ON_COMPLETE, null);
    }
    done = true;
    ON_COMPLETE_CALLS.incrementAndGet(this);

    if (fusionMode == Fuseable.ASYNC) {
      drain();
    }
  }

  public void request(long n) {
    if (Operators.validate(n)) {
      for (; ; ) {
        final long s = this.requested;
        if (s == 0) {
          this.subscription.request(n);
          return;
        }

        if ((s & Long.MIN_VALUE) != Long.MIN_VALUE) {
          return;
        }

        final long r = s & Long.MAX_VALUE;
        if (r == Long.MAX_VALUE) {
          return;
        }

        final long u = addCap(r, n);
        if (REQUESTED.compareAndSet(this, s, u | Long.MIN_VALUE)) {
          if (this.fusionMode != Fuseable.NONE) {
            drain();
          }
          return;
        }
      }
    }
  }

  public void cancel() {
    for (; ; ) {
      long s = this.requested;
      if (s == 0) {
        this.subscription.cancel();
        return;
      }

      if (REQUESTED.compareAndSet(this, s, Long.MAX_VALUE)) {
        if (this.fusionMode != Fuseable.NONE) {
          drain();
        }
        return;
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void drain() {
    final int previousState = markWorkAdded();
    if (isFinalized(previousState)) {
      ((Queue<T>) this.subscription).clear();
      return;
    }

    if (isWorkInProgress(previousState)) {
      return;
    }

    final Subscription s = this.subscription;
    final Queue<T> q = (Queue<T>) s;

    int expectedState = previousState + 1;
    for (; ; ) {
      long r = this.requested & Long.MAX_VALUE;
      long e = 0L;

      while (r != e) {
        // done has to be read before queue.poll to ensure there was no racing:
        // Thread1: <#drain>: queue.poll(null) --------------------> this.done(true)
        // Thread2: ------------------> <#onNext(V)> --> <#onComplete()>
        boolean done = this.done;

        final T t = q.poll();
        final boolean empty = t == null;

        if (checkTerminated(done, empty)) {
          if (!empty) {
            values.add(t);
          }
          return;
        }

        if (empty) {
          break;
        }

        values.add(t);

        e++;
      }

      if (r == e) {
        // done has to be read before queue.isEmpty to ensure there was no racing:
        // Thread1: <#drain>: queue.isEmpty(true) --------------------> this.done(true)
        // Thread2: --------------------> <#onNext(V)> ---> <#onComplete()>
        boolean done = this.done;
        boolean empty = q.isEmpty();

        if (checkTerminated(done, empty)) {
          return;
        }
      }

      if (e != 0) {
        ON_NEXT_CALLS.addAndGet(this, (int) e);
        if (r != Long.MAX_VALUE) {
          produce(e);
        }
      }

      expectedState = markWorkDone(expectedState);
      if (!isWorkInProgress(expectedState)) {
        return;
      }
    }
  }

  boolean checkTerminated(boolean done, boolean empty) {
    final long state = this.requested;
    if (state == Long.MAX_VALUE) {
      this.subscription.cancel();
      clearAndFinalize();
      return true;
    }

    if (done && empty) {
      clearAndFinalize();
      return true;
    }

    return false;
  }

  final void produce(long produced) {
    for (; ; ) {
      final long s = this.requested;

      if ((s & Long.MIN_VALUE) != Long.MIN_VALUE) {
        return;
      }

      final long r = s & Long.MAX_VALUE;
      if (r == Long.MAX_VALUE) {
        return;
      }

      final long u = r - produced;
      if (REQUESTED.compareAndSet(this, s, u | Long.MIN_VALUE)) {
        return;
      }
    }
  }

  @SuppressWarnings("unchecked")
  final void clearAndFinalize() {
    final Queue<T> q = (Queue<T>) this.subscription;
    for (; ; ) {
      final int state = this.wip;

      q.clear();

      if (WIP.compareAndSet(this, state, Integer.MIN_VALUE)) {
        return;
      }
    }
  }

  final int markWorkAdded() {
    for (; ; ) {
      final int state = this.wip;

      if (isFinalized(state)) {
        return state;
      }

      int nextState = state + 1;
      if ((nextState & Integer.MAX_VALUE) == 0) {
        return state;
      }

      if (WIP.compareAndSet(this, state, nextState)) {
        return state;
      }
    }
  }

  final int markWorkDone(int expectedState) {
    for (; ; ) {
      final int state = this.wip;

      if (expectedState != state) {
        return state;
      }

      if (isFinalized(state)) {
        return state;
      }

      if (WIP.compareAndSet(this, state, 0)) {
        return 0;
      }
    }
  }

  static boolean isFinalized(int state) {
    return state == Integer.MIN_VALUE;
  }

  static boolean isWorkInProgress(int state) {
    return (state & Integer.MAX_VALUE) > 0;
  }
}
