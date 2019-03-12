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

import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;

/** */
public class LimitableRequestPublisher<T> extends Flux<T> implements Subscription {
  private final Publisher<T> source;

  private final AtomicBoolean canceled;

  private final long prefetch;

  private long internalRequested;

  private long externalRequested;

  private volatile boolean subscribed;

  private volatile @Nullable Subscription internalSubscription;

  private LimitableRequestPublisher(Publisher<T> source, long prefetch) {
    this.source = source;
    this.prefetch = prefetch;
    this.canceled = new AtomicBoolean();
  }

  public static <T> LimitableRequestPublisher<T> wrap(Publisher<T> source, long prefetch) {
    return new LimitableRequestPublisher<>(source, prefetch);
  }

  @Override
  public void subscribe(CoreSubscriber<? super T> destination) {
    synchronized (this) {
      if (subscribed) {
        throw new IllegalStateException("only one subscriber at a time");
      }

      subscribed = true;
    }

    destination.onSubscribe(new InnerSubscription());
    source.subscribe(new InnerSubscriber(destination));
    increaseInternalLimit(prefetch);
  }

  public void increaseRequestLimit(long n) {
    synchronized (this) {
      externalRequested = Operators.addCap(n, externalRequested);
    }

    requestN();
  }

  public void increaseInternalLimit(long n) {
    synchronized (this) {
      internalRequested = Operators.addCap(n, internalRequested);
    }

    requestN();
  }

  @Override
  public void request(long n) {
    increaseRequestLimit(n);
  }

  private void requestN() {
    long r;
    synchronized (this) {
      if (internalSubscription == null) {
        return;
      }

      if (externalRequested != Long.MAX_VALUE || internalRequested != Long.MAX_VALUE) {
        r = Math.min(internalRequested, externalRequested);
        if (externalRequested != Long.MAX_VALUE) {
          externalRequested -= r;
        }
        if (internalRequested != Long.MAX_VALUE) {
          internalRequested -= r;
        }
      } else {
        r = Long.MAX_VALUE;
      }
    }

    if (r > 0) {
      internalSubscription.request(r);
    }
  }

  public void cancel() {
    if (canceled.compareAndSet(false, true) && internalSubscription != null) {
      internalSubscription.cancel();
      internalSubscription = null;
      subscribed = false;
    }
  }

  private class InnerSubscriber implements Subscriber<T> {
    Subscriber<? super T> destination;

    private InnerSubscriber(Subscriber<? super T> destination) {
      this.destination = destination;
    }

    @Override
    public void onSubscribe(Subscription s) {
      synchronized (LimitableRequestPublisher.this) {
        LimitableRequestPublisher.this.internalSubscription = s;

        if (canceled.get()) {
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
  }

  private class InnerSubscription implements Subscription {
    @Override
    public void request(long n) {}

    @Override
    public void cancel() {
      LimitableRequestPublisher.this.cancel();
    }
  }
}
