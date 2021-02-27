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
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.Operators;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

/**
 * A Processor implementation that takes a custom queue and allows only a single subscriber.
 *
 * <p>The implementation keeps the order of signals.
 */
public final class UnboundedProcessor extends FluxProcessor<ByteBuf, ByteBuf>
    implements Fuseable.QueueSubscription<ByteBuf>, Fuseable {

  final Queue<ByteBuf> queue;
  final Queue<ByteBuf> priorityQueue;

  boolean done;
  Throwable error;
  CoreSubscriber<? super ByteBuf> actual;

  static final long STATE_TERMINATED =
      0b1000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
  static final long FLAG_DISPOSED =
      0b0100_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
  static final long FLAG_CANCELLED =
      0b0010_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
  static final long FLAG_SUBSCRIBED_ONCE =
      0b0001_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000L;
  static final long MAX_WIP_VALUE =
      0b0000_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111_1111L;

  volatile long state;

  static final AtomicLongFieldUpdater<UnboundedProcessor> STATE =
      AtomicLongFieldUpdater.newUpdater(UnboundedProcessor.class, "state");

  volatile int discardGuard;

  static final AtomicIntegerFieldUpdater<UnboundedProcessor> DISCARD_GUARD =
      AtomicIntegerFieldUpdater.newUpdater(UnboundedProcessor.class, "discardGuard");

  volatile long requested;

  static final AtomicLongFieldUpdater<UnboundedProcessor> REQUESTED =
      AtomicLongFieldUpdater.newUpdater(UnboundedProcessor.class, "requested");

  boolean outputFused;

  public UnboundedProcessor() {
    this.queue = new MpscUnboundedArrayQueue<>(Queues.SMALL_BUFFER_SIZE);
    this.priorityQueue = new MpscUnboundedArrayQueue<>(Queues.SMALL_BUFFER_SIZE);
  }

  @Override
  public int getBufferSize() {
    return Integer.MAX_VALUE;
  }

  @Override
  public Object scanUnsafe(Attr key) {
    if (Attr.BUFFERED == key) return this.queue.size() + this.priorityQueue.size();
    if (Attr.PREFETCH == key) return Integer.MAX_VALUE;
    return super.scanUnsafe(key);
  }

  public void onNextPrioritized(ByteBuf t) {
    if (this.done) {
      release(t);
      return;
    }

    if (!this.priorityQueue.offer(t)) {
      Throwable ex =
          Operators.onOperatorError(null, Exceptions.failWithOverflow(), t, currentContext());
      onError(Operators.onOperatorError(null, ex, t, currentContext()));
      release(t);
      return;
    }

    drain();
  }

  @Override
  public void onNext(ByteBuf t) {
    if (this.done) {
      release(t);
      return;
    }

    if (!this.queue.offer(t)) {
      Throwable ex =
          Operators.onOperatorError(null, Exceptions.failWithOverflow(), t, currentContext());
      onError(Operators.onOperatorError(null, ex, t, currentContext()));
      release(t);
      return;
    }

    drain();
  }

  @Override
  public void onError(Throwable t) {
    if (this.done) {
      Operators.onErrorDropped(t, currentContext());
      return;
    }

    this.error = t;
    this.done = true;

    drain();
  }

  @Override
  public void onComplete() {
    if (this.done) {
      return;
    }

    this.done = true;

    drain();
  }

  @Override
  public void subscribe(CoreSubscriber<? super ByteBuf> actual) {
    Objects.requireNonNull(actual, "subscribe");
    if (markSubscribedOnce(this)) {
      this.actual = actual;
      actual.onSubscribe(this);
      drain();
    } else {
      Operators.error(
          actual, new IllegalStateException("UnboundedProcessor allows only a single Subscriber"));
    }
  }

  void drain() {
    long previousState = wipIncrement(this);
    if (isTerminated(previousState)) {
      this.clearSafely();
      return;
    }

    if (isWorkInProgress(previousState)) {
      return;
    }

    long expectedState = previousState + 1;
    for (; ; ) {
      if (isSubscribedOnce(expectedState)) {
        final boolean outputFused = this.outputFused;
        final Subscriber<? super ByteBuf> a = this.actual;

        if (outputFused) {
          if (isCancelled(expectedState)) {
            return;
          }

          if (isDisposed(expectedState)) {
            a.onError(new CancellationException("Disposed"));
            return;
          }

          drainFused(expectedState, a);
        } else {
          if (isCancelled(expectedState)) {
            clearAndTerminate(this);
            return;
          }

          if (isDisposed(expectedState)) {
            clearAndTerminate(this);
            a.onError(new CancellationException("Disposed"));
            return;
          }

          drainRegular(expectedState, a);
        }
        return;
      } else {
        if (isCancelled(expectedState) || isDisposed(expectedState)) {
          clearAndTerminate(this);
          return;
        }
      }

      expectedState = wipRemoveMissing(this, expectedState);
      if (!isWorkInProgress(expectedState)) {
        return;
      }
    }
  }

  void drainRegular(long expectedState, Subscriber<? super ByteBuf> a) {
    final Queue<ByteBuf> q = this.queue;
    final Queue<ByteBuf> pq = this.priorityQueue;

    for (; ; ) {

      long r = this.requested;
      long e = 0L;

      while (r != e) {
        // done has to be read before queue.poll to ensure there was no racing:
        // Thread1: <#drain>: queue.poll(null) --------------------> this.done(true)
        // Thread2: ------------------> <#onNext(V)> --> <#onComplete()>
        boolean done = this.done;

        ByteBuf t;
        boolean empty;

        if (!pq.isEmpty()) {
          t = pq.poll();
          empty = false;
        } else {
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
        if (checkTerminated(this.done, q.isEmpty() && pq.isEmpty(), a)) {
          return;
        }
      }

      if (e != 0 && r != Long.MAX_VALUE) {
        REQUESTED.addAndGet(this, -e);
      }

      expectedState = wipRemoveMissing(this, expectedState);
      if (isCancelled(expectedState)) {
        clearAndTerminate(this);
        return;
      }

      if (isDisposed(expectedState)) {
        clearAndTerminate(this);
        a.onError(new CancellationException("Disposed"));
        return;
      }

      if (!isWorkInProgress(expectedState)) {
        break;
      }
    }
  }

  void drainFused(long expectedState, Subscriber<? super ByteBuf> a) {
    for (; ; ) {
      // done has to be read before queue.poll to ensure there was no racing:
      // Thread1: <#drain>: queue.poll(null) --------------------> this.done(true)
      boolean d = this.done;

      a.onNext(null);

      if (d) {
        Throwable ex = this.error;
        if (ex != null) {
          a.onError(ex);
        } else {
          a.onComplete();
        }
        return;
      }

      expectedState = wipRemoveMissing(this, expectedState);
      if (isCancelled(expectedState)) {
        return;
      }

      if (isDisposed(expectedState)) {
        a.onError(new CancellationException("Disposed"));
        return;
      }

      if (!isWorkInProgress(expectedState)) {
        break;
      }
    }
  }

  boolean checkTerminated(boolean done, boolean empty, Subscriber<? super ByteBuf> a) {
    final long state = this.state;
    if (isCancelled(state)) {
      clearAndTerminate(this);
      return true;
    }

    if (isDisposed(state)) {
      clearAndTerminate(this);
      a.onError(new CancellationException("Disposed"));
      return true;
    }

    if (done && empty) {
      Throwable e = this.error;
      if (e != null) {
        a.onError(e);
      } else {
        a.onComplete();
      }
      clearAndTerminate(this);
      return true;
    }

    return false;
  }

  @Override
  public void onSubscribe(Subscription s) {
    final long state = this.state;
    if (this.done || isTerminated(state) || isCancelled(state) || isDisposed(state)) {
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
    final long state = this.state;
    if (isSubscribedOnce(state) || isTerminated(state)) {
      CoreSubscriber<? super ByteBuf> actual = this.actual;
      return actual != null ? actual.currentContext() : Context.empty();
    }

    return Context.empty();
  }

  @Override
  public void request(long n) {
    if (Operators.validate(n)) {
      Operators.addCap(REQUESTED, this, n);
      drain();
    }
  }

  @Override
  public void cancel() {
    if (!markCancelled(this)) {
      return;
    }

    if (!this.outputFused) {
      clearAndTerminate(this);
    }
  }

  @Override
  public void dispose() {
    final long previousState = markDisposed(this);
    if (isTerminated(previousState)
        || isCancelled(previousState)
        || isDisposed(previousState)
        || isWorkInProgress(previousState)) {
      return;
    }

    if (!this.outputFused) {
      clearAndTerminate(this);
    }

    if (isSubscribedOnce(previousState)) {
      this.actual.onError(new CancellationException("Disposed"));
    }
  }

  @Override
  @Nullable
  public ByteBuf poll() {
    Queue<ByteBuf> pq = this.priorityQueue;
    if (!pq.isEmpty()) {
      return pq.poll();
    }
    return this.queue.poll();
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
    clearAndTerminate(this);
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

    ByteBuf byteBuf;
    while ((byteBuf = queue.poll()) != null) {
      release(byteBuf);
    }

    while ((byteBuf = priorityQueue.poll()) != null) {
      release(byteBuf);
    }
  }

  @Override
  public int size() {
    return this.priorityQueue.size() + this.queue.size();
  }

  @Override
  public boolean isEmpty() {
    return this.priorityQueue.isEmpty() && this.queue.isEmpty();
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
    final long state = this.state;
    return isTerminated(state) || isCancelled(state) || isDisposed(state) || this.done;
  }

  @Override
  public boolean isTerminated() {
    final long state = this.state;
    return isTerminated(state) || this.done;
  }

  @Override
  @Nullable
  public Throwable getError() {
    final long state = this.state;
    if (isTerminated(state) || this.done) {
      return this.error;
    } else {
      return null;
    }
  }

  @Override
  public long downstreamCount() {
    return hasDownstreams() ? 1L : 0L;
  }

  @Override
  public boolean hasDownstreams() {
    return (this.state & FLAG_SUBSCRIBED_ONCE) == FLAG_SUBSCRIBED_ONCE && this.actual != null;
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
   * Tries to set {@link #FLAG_SUBSCRIBED_ONCE} flag if it was not set before and if state is not
   * {@link #STATE_TERMINATED} and flags {@link #FLAG_CANCELLED} or {@link #FLAG_DISPOSED} are unset
   *
   * @return {@code true} if {@link #FLAG_SUBSCRIBED_ONCE} was successfully set
   */
  static boolean markSubscribedOnce(UnboundedProcessor instance) {
    for (; ; ) {
      long state = instance.state;

      if (state == STATE_TERMINATED) {
        return false;
      }

      if ((state & FLAG_SUBSCRIBED_ONCE) == FLAG_SUBSCRIBED_ONCE
          || (state & FLAG_CANCELLED) == FLAG_CANCELLED
          || (state & FLAG_DISPOSED) == FLAG_DISPOSED) {
        return false;
      }

      if (STATE.compareAndSet(instance, state, state | FLAG_SUBSCRIBED_ONCE)) {
        return true;
      }
    }
  }

  /**
   * Tries to set {@link #FLAG_CANCELLED} flag if it was not set before and if state is not {@link
   * #STATE_TERMINATED}. Also, this method increments number of work in progress (WIP)
   *
   * @return {@code true} if {@link #FLAG_CANCELLED} was successfully set
   */
  static boolean markCancelled(UnboundedProcessor instance) {
    for (; ; ) {
      long state = instance.state;

      if (state == STATE_TERMINATED) {
        return false;
      }

      if ((state & FLAG_CANCELLED) == FLAG_CANCELLED) {
        return false;
      }

      long nextState = state + 1;
      if ((nextState & MAX_WIP_VALUE) == 0) {
        nextState = state;
      }

      if (STATE.compareAndSet(instance, state, nextState | FLAG_CANCELLED)) {
        return !isWorkInProgress(state);
      }
    }
  }

  /**
   * Tries to set {@link #FLAG_DISPOSED} flag if it was not set before and if state is not {@link
   * #STATE_TERMINATED} and flags {@link #FLAG_CANCELLED} are unset. Also, this method increments
   * number of work in progress (WIP)
   *
   * @return previous state
   */
  static long markDisposed(UnboundedProcessor instance) {
    for (; ; ) {
      long state = instance.state;

      if (state == STATE_TERMINATED) {
        return STATE_TERMINATED;
      }

      if ((state & FLAG_CANCELLED) == FLAG_CANCELLED || (state & FLAG_DISPOSED) == FLAG_DISPOSED) {
        return state;
      }

      long nextState = state + 1;
      if ((nextState & MAX_WIP_VALUE) == 0) {
        nextState = state;
      }

      if (STATE.compareAndSet(instance, state, nextState | FLAG_DISPOSED)) {
        return state;
      }
    }
  }

  /**
   * Tries to increment the amount of work in progress (max value is {@link #MAX_WIP_VALUE} on the
   * given state. Fails if state is {@link #STATE_TERMINATED}.
   *
   * @return previous state
   */
  static long wipIncrement(UnboundedProcessor instance) {
    for (; ; ) {
      long state = instance.state;

      if (state == STATE_TERMINATED) {
        return STATE_TERMINATED;
      }

      final long nextState = state + 1;
      if ((nextState & MAX_WIP_VALUE) == 0) {
        return state;
      }

      if (STATE.compareAndSet(instance, state, nextState)) {
        return state;
      }
    }
  }

  /**
   * Tries to decrement the amount of work in progress by the given amount on the given state. Fails
   * if state is {@link #STATE_TERMINATED} or it has flags {@link #FLAG_CANCELLED} or {@link
   * #FLAG_DISPOSED} set.
   *
   * @return state after changing WIP or current state if update failed
   */
  static long wipRemoveMissing(UnboundedProcessor instance, long previousState) {
    long missed = previousState & MAX_WIP_VALUE;
    boolean outputFused = instance.outputFused;
    for (; ; ) {
      long state = instance.state;

      if (state == STATE_TERMINATED) {
        return STATE_TERMINATED;
      }

      if (!outputFused
          && ((state & FLAG_CANCELLED) == FLAG_CANCELLED
              || (state & FLAG_DISPOSED) == FLAG_DISPOSED)) {
        return state;
      }

      final long nextState = state - missed;
      if (STATE.compareAndSet(instance, state, nextState)) {
        return nextState;
      }
    }
  }

  /**
   * Set state {@link #STATE_TERMINATED} and {@link #release(ByteBuf)} all the elements from {@link
   * #queue} and {@link #priorityQueue}.
   *
   * <p>This method may be called concurrently only if the given {@link UnboundedProcessor} has no
   * output fusion ({@link #outputFused} {@code == true}). Otherwise this method MUST be called once
   * and only by the downstream calling method {@link #clear()}
   */
  static void clearAndTerminate(UnboundedProcessor instance) {
    final boolean outputFused = instance.outputFused;
    for (; ; ) {
      long state = instance.state;

      if (outputFused) {
        instance.clearSafely();
      } else {
        instance.clearUnsafely();
      }

      if (state == STATE_TERMINATED) {
        return;
      }

      if (STATE.compareAndSet(instance, state, STATE_TERMINATED)) {
        break;
      }
    }
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
    return state == STATE_TERMINATED;
  }

  static boolean isSubscribedOnce(long state) {
    return (state & FLAG_SUBSCRIBED_ONCE) == FLAG_SUBSCRIBED_ONCE;
  }
}
