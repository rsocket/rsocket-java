/*
 * Copyright 2015-2018 the original author or authors.
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

package io.rsocket.internal;

import io.netty.buffer.ByteBuf;
import io.rsocket.internal.jctools.queues.MpscUnboundedArrayQueue;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.stream.Stream;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

/**
 * A Processor implementation that takes a custom queue and allows only a single subscriber.
 *
 * <p>The implementation keeps the order of signals.
 */
public final class UnboundedProcessor extends Flux<ByteBuf>
    implements Scannable,
        Disposable,
        CoreSubscriber<ByteBuf>,
        Fuseable.QueueSubscription<ByteBuf>,
        Fuseable {

  final Queue<ByteBuf> queue;
  final Queue<ByteBuf> priorityQueue;
  final Runnable onFinalizedHook;

  boolean cancelled;
  boolean done;
  Throwable error;
  CoreSubscriber<? super ByteBuf> actual;

  static final long FLAG_FINALIZED =
      0b1000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
  static final long FLAG_DISPOSED =
      0b0100_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
  static final long FLAG_TERMINATED =
      0b0010_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
  static final long FLAG_CANCELLED =
      0b0001_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
  static final long FLAG_HAS_VALUE =
      0b0000_1000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
  static final long FLAG_HAS_REQUEST =
      0b0000_0100_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
  static final long FLAG_SUBSCRIBER_READY =
      0b0000_0010_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
  static final long FLAG_SUBSCRIBED_ONCE =
      0b0000_0001_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
  static final long MAX_WIP_VALUE =
      0b0000_0000_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111L;

  volatile long state;

  static final AtomicLongFieldUpdater<UnboundedProcessor> STATE =
      AtomicLongFieldUpdater.newUpdater(UnboundedProcessor.class, "state");

  volatile int discardGuard;

  static final AtomicIntegerFieldUpdater<UnboundedProcessor> DISCARD_GUARD =
      AtomicIntegerFieldUpdater.newUpdater(UnboundedProcessor.class, "discardGuard");

  volatile long requested;

  static final AtomicLongFieldUpdater<UnboundedProcessor> REQUESTED =
      AtomicLongFieldUpdater.newUpdater(UnboundedProcessor.class, "requested");

  ByteBuf last;

  boolean outputFused;

  public UnboundedProcessor() {
    this(() -> {});
  }

  public UnboundedProcessor(Runnable onFinalizedHook) {
    this.onFinalizedHook = onFinalizedHook;
    this.queue = new MpscUnboundedArrayQueue<>(Queues.SMALL_BUFFER_SIZE);
    this.priorityQueue = new MpscUnboundedArrayQueue<>(Queues.SMALL_BUFFER_SIZE);
  }

  @Override
  public Stream<Scannable> inners() {
    return hasDownstreams() ? Stream.of(Scannable.from(this.actual)) : Stream.empty();
  }

  @Override
  public Object scanUnsafe(Attr key) {
    if (Attr.ACTUAL == key) return isSubscriberReady(this.state) ? this.actual : null;
    if (Attr.BUFFERED == key) return this.queue.size() + this.priorityQueue.size();
    if (Attr.PREFETCH == key) return Integer.MAX_VALUE;
    if (Attr.CANCELLED == key) {
      final long state = this.state;
      return isCancelled(state) || isDisposed(state);
    }

    return null;
  }

  public boolean tryEmitPrioritized(ByteBuf t) {
    if (this.done || this.cancelled) {
      release(t);
      return false;
    }

    if (!this.priorityQueue.offer(t)) {
      onError(Operators.onOperatorError(null, Exceptions.failWithOverflow(), t, currentContext()));
      release(t);
      return false;
    }

    final long previousState = markValueAdded(this);
    if (isFinalized(previousState)) {
      this.clearSafely();
      return false;
    }

    if (isSubscriberReady(previousState)) {
      if (this.outputFused) {
        // fast path for fusion
        this.actual.onNext(null);
        return true;
      }

      if (isWorkInProgress(previousState)) {
        return true;
      }

      if (hasRequest(previousState)) {
        drainRegular(previousState);
      }
    }
    return true;
  }

  public boolean tryEmitNormal(ByteBuf t) {
    if (this.done || this.cancelled) {
      release(t);
      return false;
    }

    if (!this.queue.offer(t)) {
      onError(Operators.onOperatorError(null, Exceptions.failWithOverflow(), t, currentContext()));
      release(t);
      return false;
    }

    final long previousState = markValueAdded(this);
    if (isFinalized(previousState)) {
      this.clearSafely();
      return false;
    }

    if (isSubscriberReady(previousState)) {
      if (this.outputFused) {
        // fast path for fusion
        this.actual.onNext(null);
        return true;
      }

      if (isWorkInProgress(previousState)) {
        return true;
      }

      if (hasRequest(previousState)) {
        drainRegular(previousState);
      }
    }

    return true;
  }

  public boolean tryEmitFinal(ByteBuf t) {
    if (this.done || this.cancelled) {
      release(t);
      return false;
    }

    this.last = t;
    this.done = true;

    final long previousState = markValueAddedAndTerminated(this);
    if (isFinalized(previousState)) {
      this.clearSafely();
      return false;
    }

    if (isSubscriberReady(previousState)) {
      if (this.outputFused) {
        // fast path for fusion
        this.actual.onNext(null);
        this.actual.onComplete();
        return true;
      }

      if (isWorkInProgress(previousState)) {
        return true;
      }

      if (hasRequest(previousState)) {
        drainRegular(previousState);
      }
    }

    return true;
  }

  @Deprecated
  public void onNextPrioritized(ByteBuf t) {
    tryEmitPrioritized(t);
  }

  @Override
  @Deprecated
  public void onNext(ByteBuf t) {
    tryEmitNormal(t);
  }

  @Override
  @Deprecated
  public void onError(Throwable t) {
    if (this.done || this.cancelled) {
      Operators.onErrorDropped(t, currentContext());
      return;
    }

    this.error = t;
    this.done = true;

    final long previousState = markTerminatedOrFinalized(this);
    if (isFinalized(previousState)
        || isDisposed(previousState)
        || isCancelled(previousState)
        || isTerminated(previousState)) {
      Operators.onErrorDropped(t, currentContext());
      return;
    }

    if (isSubscriberReady(previousState)) {
      if (this.outputFused) {
        // fast path for fusion scenario
        this.actual.onError(t);
        return;
      }

      if (isWorkInProgress(previousState)) {
        return;
      }

      if (!hasValue(previousState)) {
        // fast path no-values scenario
        this.actual.onError(t);
        return;
      }

      if (hasRequest(previousState)) {
        drainRegular(previousState);
      }
    }
  }

  @Override
  @Deprecated
  public void onComplete() {
    if (this.done || this.cancelled) {
      return;
    }

    this.done = true;

    final long previousState = markTerminatedOrFinalized(this);
    if (isFinalized(previousState)
        || isDisposed(previousState)
        || isCancelled(previousState)
        || isTerminated(previousState)) {
      return;
    }

    if (isSubscriberReady(previousState)) {
      if (this.outputFused) {
        // fast path for fusion scenario
        this.actual.onComplete();
        return;
      }

      if (isWorkInProgress(previousState)) {
        return;
      }

      if (!hasValue(previousState)) {
        this.actual.onComplete();
        return;
      }

      if (hasRequest(previousState)) {
        drainRegular(previousState);
      }
    }
  }

  void drainRegular(long previousState) {
    final CoreSubscriber<? super ByteBuf> a = this.actual;
    final Queue<ByteBuf> q = this.queue;
    final Queue<ByteBuf> pq = this.priorityQueue;

    long expectedState = previousState + 1;
    for (; ; ) {

      long r = this.requested;
      long e = 0L;

      boolean empty = false;
      boolean done;
      while (r != e) {
        // done has to be read before queue.poll to ensure there was no racing:
        // Thread1: <#drain>: queue.poll(null) --------------------> this.done(true)
        // Thread2: ------------------> <#onNext(V)> --> <#onComplete()>
        done = this.done;

        ByteBuf t = pq.poll();
        empty = t == null;

        if (empty) {
          t = q.poll();
          empty = t == null;
        }

        if (checkTerminated(done, empty, a)) {
          if (!empty) {
            release(t);
          }
          return;
        }

        if (empty) {
          break;
        }

        a.onNext(t);

        e++;
      }

      if (r == e) {
        // done has to be read before queue.isEmpty to ensure there was no racing:
        // Thread1: <#drain>: queue.isEmpty(true) --------------------> this.done(true)
        // Thread2: --------------------> <#onNext(V)> ---> <#onComplete()>
        done = this.done;
        empty = q.isEmpty() && pq.isEmpty();

        if (checkTerminated(done, empty, a)) {
          return;
        }
      }

      if (e != 0 && r != Long.MAX_VALUE) {
        r = REQUESTED.addAndGet(this, -e);
      }

      expectedState = markWorkDone(this, expectedState, r > 0, !empty);
      if (isCancelled(expectedState)) {
        clearAndFinalize(this);
        return;
      }

      if (isDisposed(expectedState)) {
        clearAndFinalize(this);
        a.onError(new CancellationException("Disposed"));
        return;
      }

      if (!isWorkInProgress(expectedState)) {
        break;
      }
    }
  }

  boolean checkTerminated(boolean done, boolean empty, CoreSubscriber<? super ByteBuf> a) {
    final long state = this.state;
    if (isCancelled(state)) {
      clearAndFinalize(this);
      return true;
    }

    if (isDisposed(state)) {
      clearAndFinalize(this);
      a.onError(new CancellationException("Disposed"));
      return true;
    }

    if (done && empty) {
      final ByteBuf last = this.last;
      if (last != null) {
        this.last = null;
        a.onNext(last);
      }
      clearAndFinalize(this);
      Throwable e = this.error;
      if (e != null) {
        a.onError(e);
      } else {
        a.onComplete();
      }
      return true;
    }

    return false;
  }

  @Override
  public void onSubscribe(Subscription s) {
    final long state = this.state;
    if (isFinalized(state) || isTerminated(state) || isCancelled(state) || isDisposed(state)) {
      s.cancel();
    } else {
      s.request(Long.MAX_VALUE);
    }
  }

  @Override
  public int getPrefetch() {
    return Integer.MAX_VALUE;
  }

  @Override
  public Context currentContext() {
    return isSubscriberReady(this.state) ? this.actual.currentContext() : Context.empty();
  }

  @Override
  public void subscribe(CoreSubscriber<? super ByteBuf> actual) {
    Objects.requireNonNull(actual, "subscribe");
    long previousState = markSubscribedOnce(this);
    if (isSubscribedOnce(previousState)) {
      Operators.error(
          actual, new IllegalStateException("UnboundedProcessor allows only a single Subscriber"));
      return;
    }

    if (isDisposed(previousState)) {
      Operators.error(actual, new CancellationException("Disposed"));
      return;
    }

    actual.onSubscribe(this);
    this.actual = actual;

    previousState = markSubscriberReady(this);

    if (this.outputFused) {
      if (isCancelled(previousState)) {
        return;
      }

      if (isDisposed(previousState)) {
        actual.onError(new CancellationException("Disposed"));
        return;
      }

      if (hasValue(previousState)) {
        actual.onNext(null);
      }

      if (isTerminated(previousState)) {
        final Throwable e = this.error;
        if (e != null) {
          actual.onError(e);
        } else {
          actual.onComplete();
        }
      }
      return;
    }

    if (isCancelled(previousState)) {
      clearAndFinalize(this);
      return;
    }

    if (isDisposed(previousState)) {
      clearAndFinalize(this);
      actual.onError(new CancellationException("Disposed"));
      return;
    }

    if (!hasValue(previousState)) {
      if (isTerminated(previousState)) {
        clearAndFinalize(this);
        final Throwable e = this.error;
        if (e != null) {
          actual.onError(e);
        } else {
          actual.onComplete();
        }
      }
      return;
    }

    if (hasRequest(previousState)) {
      drainRegular(previousState);
    }
  }

  @Override
  public void request(long n) {
    if (Operators.validate(n)) {
      if (this.outputFused) {
        final long state = this.state;
        if (isSubscriberReady(state)) {
          this.actual.onNext(null);
        }
        return;
      }

      Operators.addCap(REQUESTED, this, n);

      final long previousState = markRequestAdded(this);
      if (isWorkInProgress(previousState)
          || isFinalized(previousState)
          || isCancelled(previousState)
          || isDisposed(previousState)) {
        return;
      }

      if (isSubscriberReady(previousState) && hasValue(previousState)) {
        drainRegular(previousState);
      }
    }
  }

  @Override
  public void cancel() {
    this.cancelled = true;

    final long previousState = markCancelled(this);
    if (isWorkInProgress(previousState)
        || isFinalized(previousState)
        || isCancelled(previousState)
        || isDisposed(previousState)) {
      return;
    }

    if (!isSubscribedOnce(previousState) || !this.outputFused) {
      clearAndFinalize(this);
    }
  }

  @Override
  @Deprecated
  public void dispose() {
    this.cancelled = true;

    final long previousState = markDisposed(this);
    if (isWorkInProgress(previousState)
        || isFinalized(previousState)
        || isCancelled(previousState)
        || isDisposed(previousState)) {
      return;
    }

    if (!isSubscribedOnce(previousState)) {
      clearAndFinalize(this);
      return;
    }

    if (!isSubscriberReady(previousState)) {
      return;
    }

    if (!this.outputFused) {
      clearAndFinalize(this);
      this.actual.onError(new CancellationException("Disposed"));
      return;
    }

    if (!isTerminated(previousState)) {
      this.actual.onError(new CancellationException("Disposed"));
    }
  }

  @Override
  @Nullable
  public ByteBuf poll() {
    ByteBuf t = this.priorityQueue.poll();
    if (t != null) {
      return t;
    }

    t = this.queue.poll();
    if (t != null) {
      return t;
    }

    t = this.last;
    if (t != null) {
      this.last = null;
      return t;
    }

    return null;
  }

  @Override
  public int size() {
    return this.priorityQueue.size() + this.queue.size();
  }

  @Override
  public boolean isEmpty() {
    return this.priorityQueue.isEmpty() && this.queue.isEmpty();
  }

  /**
   * Clears all elements from queues and set state to terminate. This method MUST be called only by
   * the downstream subscriber which has enabled {@link Fuseable#ASYNC} fusion with the given {@link
   * UnboundedProcessor} and is and indicator that the downstream is done with draining, it has
   * observed any terminal signal (ON_COMPLETE or ON_ERROR or CANCEL) and will never be interacting
   * with SingleConsumer queue anymore.
   */
  @Override
  public void clear() {
    clearAndFinalize(this);
  }

  void clearSafely() {
    if (DISCARD_GUARD.getAndIncrement(this) != 0) {
      return;
    }

    int missed = 1;
    for (; ; ) {
      clearUnsafely();

      missed = DISCARD_GUARD.addAndGet(this, -missed);
      if (missed == 0) {
        break;
      }
    }
  }

  void clearUnsafely() {
    final Queue<ByteBuf> queue = this.queue;
    final Queue<ByteBuf> priorityQueue = this.priorityQueue;

    final ByteBuf last = this.last;

    if (last != null) {
      release(last);
    }

    ByteBuf byteBuf;
    while ((byteBuf = queue.poll()) != null) {
      release(byteBuf);
    }

    while ((byteBuf = priorityQueue.poll()) != null) {
      release(byteBuf);
    }
  }

  @Override
  public int requestFusion(int requestedMode) {
    if ((requestedMode & Fuseable.ASYNC) != 0) {
      this.outputFused = true;
      return Fuseable.ASYNC;
    }
    return Fuseable.NONE;
  }

  @Override
  public boolean isDisposed() {
    return isFinalized(this.state);
  }

  boolean hasDownstreams() {
    final long state = this.state;
    return !isTerminated(state) && isSubscriberReady(state);
  }

  static void release(ByteBuf byteBuf) {
    if (byteBuf.refCnt() > 0) {
      try {
        byteBuf.release();
      } catch (Throwable ex) {
        // no ops
      }
    }
  }

  /**
   * Sets {@link #FLAG_SUBSCRIBED_ONCE} flag if it was not set before and if flags {@link
   * #FLAG_FINALIZED}, {@link #FLAG_CANCELLED} or {@link #FLAG_DISPOSED} are unset
   *
   * @return {@code true} if {@link #FLAG_SUBSCRIBED_ONCE} was successfully set
   */
  static long markSubscribedOnce(UnboundedProcessor instance) {
    for (; ; ) {
      final long state = instance.state;

      if (isSubscribedOnce(state)) {
        return state;
      }

      if (STATE.compareAndSet(instance, state, state | FLAG_SUBSCRIBED_ONCE)) {
        return state;
      }
    }
  }

  /**
   * Sets {@link #FLAG_SUBSCRIBER_READY} flag if flags {@link #FLAG_FINALIZED}, {@link
   * #FLAG_CANCELLED} or {@link #FLAG_DISPOSED} are unset
   *
   * @return previous state
   */
  static long markSubscriberReady(UnboundedProcessor instance) {
    for (; ; ) {
      long state = instance.state;

      if (isFinalized(state) || isCancelled(state) || isDisposed(state)) {
        return state;
      }

      long nextState = state;
      if (!instance.outputFused) {
        if ((!hasValue(state) && isTerminated(state)) || (hasRequest(state) && hasValue(state))) {
          nextState = addWork(state);
        }
      }

      if (STATE.compareAndSet(instance, state, nextState | FLAG_SUBSCRIBER_READY)) {
        return state;
      }
    }
  }

  /**
   * Sets {@link #FLAG_HAS_REQUEST} flag if it was not set before and if flags {@link
   * #FLAG_FINALIZED}, {@link #FLAG_CANCELLED}, {@link #FLAG_DISPOSED} are unset. Also, this method
   * increments number of work in progress (WIP)
   *
   * @return previous state
   */
  static long markRequestAdded(UnboundedProcessor instance) {
    for (; ; ) {
      long state = instance.state;

      if (isFinalized(state) || isCancelled(state) || isDisposed(state)) {
        return state;
      }

      long nextState = state;
      if (isSubscriberReady(state) && hasValue(state)) {
        nextState = addWork(state);
      }

      if (STATE.compareAndSet(instance, state, nextState | FLAG_HAS_REQUEST)) {
        return state;
      }
    }
  }

  /**
   * Sets {@link #FLAG_HAS_VALUE} flag if it was not set before and if flags {@link
   * #FLAG_FINALIZED}, {@link #FLAG_CANCELLED}, {@link #FLAG_DISPOSED} are unset. Also, this method
   * increments number of work in progress (WIP) if {@link #FLAG_HAS_REQUEST} is set
   *
   * @return previous state
   */
  static long markValueAdded(UnboundedProcessor instance) {
    for (; ; ) {
      final long state = instance.state;

      if (isFinalized(state)) {
        return state;
      }

      long nextState = state;
      if (isWorkInProgress(state)) {
        nextState = addWork(state);
      } else if (isSubscriberReady(state)) {
        if (instance.outputFused) {
          // fast path for fusion scenario
          return state;
        }

        if (hasRequest(state)) {
          nextState = addWork(state);
        }
      }

      if (STATE.compareAndSet(instance, state, nextState | FLAG_HAS_VALUE)) {
        return state;
      }
    }
  }

  /**
   * Sets {@link #FLAG_HAS_VALUE} flag if it was not set before and if flags {@link
   * #FLAG_FINALIZED}, {@link #FLAG_CANCELLED}, {@link #FLAG_DISPOSED} are unset. Also, this method
   * increments number of work in progress (WIP) if {@link #FLAG_HAS_REQUEST} is set
   *
   * @return previous state
   */
  static long markValueAddedAndTerminated(UnboundedProcessor instance) {
    for (; ; ) {
      final long state = instance.state;

      if (isFinalized(state)) {
        return state;
      }

      long nextState = state;
      if (isWorkInProgress(state)) {
        nextState = addWork(state);
      } else if (isSubscriberReady(state) && !instance.outputFused) {
        if (hasRequest(state)) {
          nextState = addWork(state);
        }
      }

      if (STATE.compareAndSet(instance, state, nextState | FLAG_HAS_VALUE | FLAG_TERMINATED)) {
        return state;
      }
    }
  }

  /**
   * Sets {@link #FLAG_TERMINATED} flag if it was not set before and if flags {@link
   * #FLAG_FINALIZED}, {@link #FLAG_CANCELLED}, {@link #FLAG_DISPOSED} are unset. Also, this method
   * increments number of work in progress (WIP)
   *
   * @return previous state
   */
  static long markTerminatedOrFinalized(UnboundedProcessor instance) {
    for (; ; ) {
      final long state = instance.state;

      if (isFinalized(state) || isTerminated(state) || isCancelled(state) || isDisposed(state)) {
        return state;
      }

      long nextState = state;
      if (isSubscriberReady(state) && !instance.outputFused) {
        if (!hasValue(state)) {
          // fast path for no values and no work in progress
          nextState = FLAG_FINALIZED;
        } else if (hasRequest(state)) {
          nextState = addWork(state);
        }
      }

      if (STATE.compareAndSet(instance, state, nextState | FLAG_TERMINATED)) {
        if (isFinalized(nextState)) {
          instance.onFinalizedHook.run();
        }
        return state;
      }
    }
  }

  /**
   * Sets {@link #FLAG_CANCELLED} flag if it was not set before and if flag {@link #FLAG_FINALIZED}
   * is unset. Also, this method increments number of work in progress (WIP)
   *
   * @return previous state
   */
  static long markCancelled(UnboundedProcessor instance) {
    for (; ; ) {
      final long state = instance.state;

      if (isFinalized(state) || isCancelled(state)) {
        return state;
      }

      final long nextState = addWork(state);
      if (STATE.compareAndSet(instance, state, nextState | FLAG_CANCELLED)) {
        return state;
      }
    }
  }

  /**
   * Sets {@link #FLAG_DISPOSED} flag if it was not set before and if flags {@link #FLAG_FINALIZED},
   * {@link #FLAG_CANCELLED} are unset. Also, this method increments number of work in progress
   * (WIP)
   *
   * @return previous state
   */
  static long markDisposed(UnboundedProcessor instance) {
    for (; ; ) {
      final long state = instance.state;

      if (isFinalized(state) || isCancelled(state) || isDisposed(state)) {
        return state;
      }

      final long nextState = addWork(state);
      if (STATE.compareAndSet(instance, state, nextState | FLAG_DISPOSED)) {
        return state;
      }
    }
  }

  static long addWork(long state) {
    return (state & MAX_WIP_VALUE) == MAX_WIP_VALUE ? state : state + 1;
  }

  /**
   * Decrements the amount of work in progress by the given amount on the given state. Fails if flag
   * is {@link #FLAG_FINALIZED} is set or if fusion disabled and flags {@link #FLAG_CANCELLED} or
   * {@link #FLAG_DISPOSED} are set.
   *
   * <p>Note, if fusion is enabled, the decrement should work if flags {@link #FLAG_CANCELLED} or
   * {@link #FLAG_DISPOSED} are set, since, while the operator was not terminate by the downstream,
   * we still have to propagate notifications that new elements are enqueued
   *
   * @return state after changing WIP or current state if update failed
   */
  static long markWorkDone(
      UnboundedProcessor instance, long expectedState, boolean hasRequest, boolean hasValue) {
    final long expectedMissed = expectedState & MAX_WIP_VALUE;
    for (; ; ) {
      final long state = instance.state;
      final long missed = state & MAX_WIP_VALUE;

      if (missed != expectedMissed) {
        return state;
      }

      if (isFinalized(state) || isCancelled(state) || isDisposed(state)) {
        return state;
      }

      final long nextState = state - expectedMissed;
      if (STATE.compareAndSet(
          instance,
          state,
          nextState ^ (hasRequest ? 0 : FLAG_HAS_REQUEST) ^ (hasValue ? 0 : FLAG_HAS_VALUE))) {
        return nextState;
      }
    }
  }

  /**
   * Set flag {@link #FLAG_FINALIZED} and {@link #release(ByteBuf)} all the elements from {@link
   * #queue} and {@link #priorityQueue}.
   *
   * <p>This method may be called concurrently only if the given {@link UnboundedProcessor} has no
   * output fusion ({@link #outputFused} {@code == true}). Otherwise this method MUST only by the
   * downstream calling method {@link #clear()}
   */
  static void clearAndFinalize(UnboundedProcessor instance) {
    for (; ; ) {
      final long state = instance.state;

      if (isFinalized(state)) {
        instance.clearSafely();
        return;
      }

      if (!isSubscriberReady(state) || !instance.outputFused) {
        instance.clearSafely();
      } else {
        instance.clearUnsafely();
      }

      if (STATE.compareAndSet(
          instance, state, (state & ~MAX_WIP_VALUE & ~FLAG_HAS_VALUE) | FLAG_FINALIZED)) {
        instance.onFinalizedHook.run();
        break;
      }
    }
  }

  static boolean hasValue(long state) {
    return (state & FLAG_HAS_VALUE) == FLAG_HAS_VALUE;
  }

  static boolean hasRequest(long state) {
    return (state & FLAG_HAS_REQUEST) == FLAG_HAS_REQUEST;
  }

  static boolean isCancelled(long state) {
    return (state & FLAG_CANCELLED) == FLAG_CANCELLED;
  }

  static boolean isDisposed(long state) {
    return (state & FLAG_DISPOSED) == FLAG_DISPOSED;
  }

  static boolean isWorkInProgress(long state) {
    return (state & MAX_WIP_VALUE) != 0;
  }

  static boolean isTerminated(long state) {
    return (state & FLAG_TERMINATED) == FLAG_TERMINATED;
  }

  static boolean isFinalized(long state) {
    return (state & FLAG_FINALIZED) == FLAG_FINALIZED;
  }

  static boolean isSubscriberReady(long state) {
    return (state & FLAG_SUBSCRIBER_READY) == FLAG_SUBSCRIBER_READY;
  }

  static boolean isSubscribedOnce(long state) {
    return (state & FLAG_SUBSCRIBED_ONCE) == FLAG_SUBSCRIBED_ONCE;
  }
}
