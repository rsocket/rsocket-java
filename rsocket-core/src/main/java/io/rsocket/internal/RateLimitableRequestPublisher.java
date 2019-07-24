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
import javax.annotation.Nullable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;

/** */
public class RateLimitableRequestPublisher<T> extends Flux<T> implements Subscription {

  private static final int NOT_CANCELED_STATE = 0;
  private static final int CANCELED_STATE = 1;

  private final Publisher<T> source;

  private volatile int canceled;
  private static final AtomicIntegerFieldUpdater<RateLimitableRequestPublisher> CANCELED =
      AtomicIntegerFieldUpdater.newUpdater(RateLimitableRequestPublisher.class, "canceled");

  private final long prefetch;
  private final long limit;

  private long externalRequested; // need sync
  private int pendingToFulfil; // need sync since should be checked/zerroed in onNext
  // and increased in request
  private int deliveredElements; // no need to sync since increased zerroed only in
  // the request method

  private boolean subscribed;

  private @Nullable Subscription internalSubscription;

  private RateLimitableRequestPublisher(Publisher<T> source, long prefetch) {
    this.source = source;
    this.prefetch = prefetch;
    this.limit = prefetch == Integer.MAX_VALUE ? Integer.MAX_VALUE : (prefetch - (prefetch >> 2));
  }

  public static <T> RateLimitableRequestPublisher<T> wrap(Publisher<T> source, long prefetch) {
    return new RateLimitableRequestPublisher<>(source, prefetch);
  }

  //  public static <T> RateLimitableRequestPublisher<T> wrap(Publisher<T> source) {
  //    return wrap(source, Long.MAX_VALUE);
  //  }

  @Override
  public void subscribe(CoreSubscriber<? super T> destination) {
    synchronized (this) {
      if (subscribed) {
        throw new IllegalStateException("only one subscriber at a time");
      }

      subscribed = true;
    }
    final InnerOperator s = new InnerOperator(destination);

    source.subscribe(s);
    destination.onSubscribe(s);
  }

  @Override
  public void request(long n) {
    synchronized (this) {
      long requested = externalRequested;
      if (requested == Long.MAX_VALUE) {
        return;
      }
      externalRequested = Operators.addCap(n, requested);
    }

    requestN();
  }

  private void requestN() {
    final long r;
    final Subscription s;

    synchronized (this) {
      s = internalSubscription;
      if (s == null) {
        return;
      }

      final long er = externalRequested;

      if (er != Long.MAX_VALUE && prefetch != Integer.MAX_VALUE) {
        // shortcut
        if (pendingToFulfil == prefetch) {
          return;
        }

        r = Math.min(prefetch - pendingToFulfil, er);
        externalRequested -= r;
        pendingToFulfil += r;
      } else {
        r = Long.MAX_VALUE;
      }
    }

    if (r > 0) {
      s.request(r);
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
    return canceled == CANCELED_STATE;
  }

  private class InnerOperator implements CoreSubscriber<T>, Subscription {
    final Subscriber<? super T> destination;

    private InnerOperator(Subscriber<? super T> destination) {
      this.destination = destination;
    }

    @Override
    public void onSubscribe(Subscription s) {
      synchronized (RateLimitableRequestPublisher.this) {
        RateLimitableRequestPublisher.this.internalSubscription = s;

        if (isCanceled()) {
          s.cancel();
          subscribed = false;
          RateLimitableRequestPublisher.this.internalSubscription = null;
        }
      }

      requestN();
    }

    @Override
    public void onNext(T t) {
      try {
        destination.onNext(t);
        deliveredElements++;

        if (deliveredElements == limit) {
          deliveredElements = 0;
          final long r;
          final Subscription s;

          synchronized (RateLimitableRequestPublisher.this) {
            s = internalSubscription;

            if (s == null) {
              return;
            }

            if (externalRequested >= limit) {
              externalRequested -= limit;
              // keep pendingToFulfil as is since it is eq to prefetch
              r = limit;
            } else {
              pendingToFulfil -= limit;
              if (externalRequested > 0) {
                r = externalRequested;
                externalRequested = 0;
                pendingToFulfil += r;
              } else {
                r = 0;
              }
            }
          }

          if (r > 0) {
            s.request(r);
          }
        }
        //        else if (deliveredElements == pendingToFulfil) {
        //          deliveredElements = 0;
        //          synchronized (RateLimitableRequestPublisher.this) {
        //            pendingToFulfil -= deliveredElements;
        //            if (deliveredElements == pendingToFulfil) {
        //              return;
        //            }
        //          }
        //        }
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
      RateLimitableRequestPublisher.this.cancel();
    }
  }
}
