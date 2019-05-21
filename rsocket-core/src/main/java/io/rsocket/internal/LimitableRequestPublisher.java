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
public class LimitableRequestPublisher<T> extends Flux<T> implements Subscription {

  private static final int NOT_CANCELED_STATE = 0;
  private static final int CANCELED_STATE = 1;

  private final Publisher<T> source;

  private volatile int canceled;
  private static final AtomicIntegerFieldUpdater<LimitableRequestPublisher> CANCELED =
      AtomicIntegerFieldUpdater.newUpdater(LimitableRequestPublisher.class, "canceled");

  private final long prefetch;

  private long internalRequested;

  private long externalRequested;

  private boolean subscribed;

  private @Nullable Subscription internalSubscription;

  private LimitableRequestPublisher(Publisher<T> source, long prefetch) {
    this.source = source;
    this.prefetch = prefetch;
  }

  public static <T> LimitableRequestPublisher<T> wrap(Publisher<T> source, long prefetch) {
    return new LimitableRequestPublisher<>(source, prefetch);
  }

  public static <T> LimitableRequestPublisher<T> wrap(Publisher<T> source) {
    return wrap(source, Long.MAX_VALUE);
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
    increaseInternalLimit(prefetch);
  }

  public void increaseInternalLimit(long n) {
    synchronized (this) {
      long requested = internalRequested;
      if (requested == Long.MAX_VALUE) {
        return;
      }
      internalRequested = Operators.addCap(n, requested);
    }

    requestN();
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
    long r;
    final Subscription s;

    synchronized (this) {
      s = internalSubscription;
      if (s == null) {
        return;
      }

      long er = externalRequested;
      long ir = internalRequested;

      if (er != Long.MAX_VALUE || ir != Long.MAX_VALUE) {
        r = Math.min(ir, er);
        if (er != Long.MAX_VALUE) {
          externalRequested -= r;
        }
        if (ir != Long.MAX_VALUE) {
          internalRequested -= r;
        }
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
