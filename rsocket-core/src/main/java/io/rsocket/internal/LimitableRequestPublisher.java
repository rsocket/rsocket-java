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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import javax.annotation.Nullable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;

/** */
public class LimitableRequestPublisher<T> extends Flux<T> implements Subscription {

  private static final int NOT_CANCELED_STATE = 0;
  private static final int CANCELED_STATE = 1;

  private final Publisher<T> source;

  private volatile int canceled;
  private static final AtomicIntegerFieldUpdater<LimitableRequestPublisher> CANCELED =
      AtomicIntegerFieldUpdater.newUpdater(LimitableRequestPublisher.class, "canceled");

  private final int prefetch;
  private final int limit;

  private int produced;

  private volatile long internalRequested;
  private static final AtomicLongFieldUpdater<LimitableRequestPublisher> INTERNAL_REQUESTED =
      AtomicLongFieldUpdater.newUpdater(LimitableRequestPublisher.class, "internalRequested");

  private volatile long externalRequested;
  private static final AtomicLongFieldUpdater<LimitableRequestPublisher> EXTERNAL_REQUESTED =
      AtomicLongFieldUpdater.newUpdater(LimitableRequestPublisher.class, "externalRequested");

  private volatile int wip;
  private static final AtomicIntegerFieldUpdater<LimitableRequestPublisher> WIP =
      AtomicIntegerFieldUpdater.newUpdater(LimitableRequestPublisher.class, "wip");

  private boolean subscribed;

  private @Nullable Subscription internalSubscription;

  private LimitableRequestPublisher(Publisher<T> source, int prefetch) {
    this.source = source;
    this.prefetch = prefetch;
    this.limit = prefetch >> 2;
  }

  public static <T> LimitableRequestPublisher<T> wrap(Publisher<T> source, int prefetch) {
    return new LimitableRequestPublisher<>(source, prefetch);
  }

  public int getLimit() {
    return limit;
  }

  public long getInternalRequested() {
    return internalRequested;
  }

  public long getExternalRequested() {
    return externalRequested;
  }

  @Override
  public void subscribe(CoreSubscriber<? super T> destination) {
    synchronized (this) {
      if (subscribed) {
        throw new IllegalStateException("only one subscriber at a time");
      }

      subscribed = true;
    }
    final InnerOperator s = new InnerOperator(destination);

    destination.onSubscribe(s);
    source.subscribe(s);
    internalRequest(prefetch == Integer.MAX_VALUE ? Long.MAX_VALUE : prefetch);
  }

  @Override
  public int getPrefetch() {
    return prefetch;
  }

  public void internalRequest(long n) {
    Operators.addCap(INTERNAL_REQUESTED, this, n);
    requestN();
  }

  @Override
  public void request(long n) {
    //    if (Operators.addCap(EXTERNAL_REQUESTED, this, n) == 0) {
    Operators.addCap(EXTERNAL_REQUESTED, this, n);
    requestN();
    //    }
  }

  private void requestN() {
    if (WIP.getAndIncrement(this) > 0) {
      return;
    }

    long r;
    final Subscription s = internalSubscription;
    int missed = 1;

    long er = externalRequested;
    long ir = internalRequested;
    int limit = this.limit;

    while (true) {
      if (s != null) {
        if (er != Long.MAX_VALUE || ir != Long.MAX_VALUE) {
          if (ir > limit) {
            r = Math.min(ir, er);

            if (er != Long.MAX_VALUE) {
              er = EXTERNAL_REQUESTED.addAndGet(this, -r);
            }
            if (ir != Long.MAX_VALUE) {
              ir = INTERNAL_REQUESTED.addAndGet(this, -r);
            }

            if (r > 0) {
              s.request(r);
            }
          }
        } else {
          s.request(Long.MAX_VALUE);
        }
      }

      missed = WIP.addAndGet(this, -missed);
      if (missed == 0) {
        break;
      }
    }
  }

  public void cancel() {
    if (!isCanceled() && CANCELED.compareAndSet(this, NOT_CANCELED_STATE, CANCELED_STATE)) {
      Subscription s;

      synchronized (this) {
        s = internalSubscription;
        internalSubscription = null;
        subscribed = false;
      }

      if (s != null) {
        s.cancel();
      }
    }
  }

  private boolean isCanceled() {
    return canceled == 1;
  }

  private class InnerOperator implements CoreSubscriber<T>, Subscription {
    final Subscriber<? super T> destination;

    private InnerOperator(Subscriber<? super T> destination) {
      this.destination = destination;
    }

    @Override
    public void onSubscribe(Subscription s) {
      synchronized (LimitableRequestPublisher.this) {
        LimitableRequestPublisher.this.internalSubscription = s;

        if (isCanceled()) {
          s.cancel();
          subscribed = false;
          LimitableRequestPublisher.this.internalSubscription = null;
        }
      }

      requestN();
    }

    @Override
    public void onNext(T t) {
      try {
        destination.onNext(t);
        int p = ++produced;

        if (p == limit) {
          produced = 0;
          requestN();
        }

      } catch (Throwable e) {
        onError(e);
      }
    }

    @Override
    public void onError(Throwable t) {
      destination.onError(t);
    }

    @Override
    public void onComplete() {
      destination.onComplete();
    }

    @Override
    public void request(long n) {}

    @Override
    public void cancel() {
      LimitableRequestPublisher.this.cancel();
    }
  }
}
