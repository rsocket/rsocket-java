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

import io.netty.util.ReferenceCounted;
import io.rsocket.internal.jctools.queues.MpscUnboundedArrayQueue;
import java.util.Objects;
import java.util.Queue;
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
 *
 * @param <T> the input and output type
 */
public final class UnboundedProcessor<T> extends FluxProcessor<T, T>
    implements Fuseable.QueueSubscription<T>, Fuseable {

  @SuppressWarnings("rawtypes")
  static final AtomicIntegerFieldUpdater<UnboundedProcessor> ONCE =
      AtomicIntegerFieldUpdater.newUpdater(UnboundedProcessor.class, "once");

  @SuppressWarnings("rawtypes")
  static final AtomicIntegerFieldUpdater<UnboundedProcessor> WIP =
      AtomicIntegerFieldUpdater.newUpdater(UnboundedProcessor.class, "wip");

  @SuppressWarnings("rawtypes")
  static final AtomicLongFieldUpdater<UnboundedProcessor> REQUESTED =
      AtomicLongFieldUpdater.newUpdater(UnboundedProcessor.class, "requested");

  final Queue<T> queue;
  volatile boolean done;
  Throwable error;
  volatile CoreSubscriber<? super T> actual;
  volatile boolean cancelled;
  volatile int once;
  volatile int wip;
  volatile long requested;
  volatile boolean outputFused;

  public UnboundedProcessor() {
    this.queue = new MpscUnboundedArrayQueue<>(Queues.SMALL_BUFFER_SIZE);
  }

  @Override
  public int getBufferSize() {
    return Queues.capacity(this.queue);
  }

  @Override
  public Object scanUnsafe(Attr key) {
    if (Attr.BUFFERED == key) return queue.size();
    return super.scanUnsafe(key);
  }

  void drainRegular(Subscriber<? super T> a) {
    int missed = 1;

    final Queue<T> q = queue;

    for (; ; ) {

      long r = requested;
      long e = 0L;

      while (r != e) {
        boolean d = done;

        T t = q.poll();
        boolean empty = t == null;

        if (checkTerminated(d, empty, a, q)) {
          return;
        }

        if (empty) {
          break;
        }

        a.onNext(t);

        e++;
      }

      if (r == e) {
        if (checkTerminated(done, q.isEmpty(), a, q)) {
          return;
        }
      }

      if (e != 0 && r != Long.MAX_VALUE) {
        REQUESTED.addAndGet(this, -e);
      }

      missed = WIP.addAndGet(this, -missed);
      if (missed == 0) {
        break;
      }
    }
  }

  void drainFused(Subscriber<? super T> a) {
    int missed = 1;

    final Queue<T> q = queue;

    for (; ; ) {

      if (cancelled) {
        q.clear();
        actual = null;
        return;
      }

      boolean d = done;

      a.onNext(null);

      if (d) {
        actual = null;

        Throwable ex = error;
        if (ex != null) {
          a.onError(ex);
        } else {
          a.onComplete();
        }
        return;
      }

      missed = WIP.addAndGet(this, -missed);
      if (missed == 0) {
        break;
      }
    }
  }

  public void drain() {
    if (WIP.getAndIncrement(this) != 0) {
      return;
    }

    int missed = 1;

    for (; ; ) {
      Subscriber<? super T> a = actual;
      if (a != null) {

        if (outputFused) {
          drainFused(a);
        } else {
          drainRegular(a);
        }
        return;
      }

      missed = WIP.addAndGet(this, -missed);
      if (missed == 0) {
        break;
      }
    }
  }

  boolean checkTerminated(boolean d, boolean empty, Subscriber<? super T> a, Queue<T> q) {
    if (cancelled) {
      while (!q.isEmpty()) {
        T t = q.poll();
        if (t != null) {
          release(t);
        }
      }
      actual = null;
      return true;
    }
    if (d && empty) {
      Throwable e = error;
      actual = null;
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
    if (done || cancelled) {
      s.cancel();
    } else {
      s.request(Long.MAX_VALUE);
    }
  }

  public long available() {
    return requested;
  }

  @Override
  public int getPrefetch() {
    return Integer.MAX_VALUE;
  }

  @Override
  public Context currentContext() {
    CoreSubscriber<? super T> actual = this.actual;
    return actual != null ? actual.currentContext() : Context.empty();
  }

  @Override
  public void onNext(T t) {
    if (done || cancelled) {
      Operators.onNextDropped(t, currentContext());
      release(t);
      return;
    }

    if (!queue.offer(t)) {
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
    if (done || cancelled) {
      Operators.onErrorDropped(t, currentContext());
      return;
    }

    error = t;
    done = true;

    drain();
  }

  @Override
  public void onComplete() {
    if (done || cancelled) {
      return;
    }

    done = true;

    drain();
  }

  @Override
  public void subscribe(CoreSubscriber<? super T> actual) {
    Objects.requireNonNull(actual, "subscribe");
    if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {

      actual.onSubscribe(this);
      this.actual = actual;
      if (cancelled) {
        this.actual = null;
      } else {
        drain();
      }
    } else {
      Operators.error(
          actual,
          new IllegalStateException("UnboundedProcessor " + "allows only a single Subscriber"));
    }
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
    if (cancelled) {
      return;
    }
    cancelled = true;

    if (WIP.getAndIncrement(this) == 0) {
      clear();
      actual = null;
    }
  }

  @Override
  public T peek() {
    return queue.peek();
  }

  @Override
  @Nullable
  public T poll() {
    return queue.poll();
  }

  @Override
  public int size() {
    return queue.size();
  }

  @Override
  public boolean isEmpty() {
    return queue.isEmpty();
  }

  @Override
  public void clear() {
    while (!queue.isEmpty()) {
      T t = queue.poll();
      if (t != null) {
        release(t);
      }
    }
  }

  @Override
  public int requestFusion(int requestedMode) {
    if ((requestedMode & Fuseable.ASYNC) != 0) {
      outputFused = true;
      return Fuseable.ASYNC;
    }
    return Fuseable.NONE;
  }

  @Override
  public void dispose() {
    cancel();
  }

  @Override
  public boolean isDisposed() {
    return cancelled || done;
  }

  @Override
  public boolean isTerminated() {
    return done;
  }

  @Override
  @Nullable
  public Throwable getError() {
    return error;
  }

  @Override
  public long downstreamCount() {
    return hasDownstreams() ? 1L : 0L;
  }

  @Override
  public boolean hasDownstreams() {
    return actual != null;
  }

  void release(T t) {
    if (t instanceof ReferenceCounted) {
      ReferenceCounted refCounted = (ReferenceCounted) t;
      if (refCounted.refCnt() > 0) {
        refCounted.release();
      }
    }
  }
}
