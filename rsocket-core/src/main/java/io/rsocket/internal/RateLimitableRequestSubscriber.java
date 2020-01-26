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

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.publisher.Operators;
import reactor.core.publisher.SignalType;

/** */
public abstract class RateLimitableRequestSubscriber<T> implements CoreSubscriber<T>, Subscription {

  private final long prefetch;
  private final long limit;

  private long externalRequested; // need sync
  private int pendingToFulfil; // need sync since should be checked/zerroed in onNext
  // and increased in request
  private int deliveredElements; // no need to sync since increased zerroed only in
  // the request method

  private volatile Subscription subscription;
  static final AtomicReferenceFieldUpdater<RateLimitableRequestSubscriber, Subscription> S =
      AtomicReferenceFieldUpdater.newUpdater(
          RateLimitableRequestSubscriber.class, Subscription.class, "subscription");

  public RateLimitableRequestSubscriber(long prefetch) {
    this.prefetch = prefetch;
    this.limit = prefetch == Integer.MAX_VALUE ? Integer.MAX_VALUE : (prefetch - (prefetch >> 2));
  }

  protected void hookOnSubscribe(Subscription s) {
    // NO-OP
  }

  protected void hookOnNext(T value) {
    // NO-OP
  }

  protected void hookOnComplete() {
    // NO-OP
  }

  protected void hookOnError(Throwable throwable) {
    throw Exceptions.errorCallbackNotImplemented(throwable);
  }

  protected void hookOnCancel() {
    // NO-OP
  }

  protected void hookFinally(SignalType type) {
    // NO-OP
  }

  void safeHookFinally(SignalType type) {
    try {
      hookFinally(type);
    } catch (Throwable finallyFailure) {
      Operators.onErrorDropped(finallyFailure, currentContext());
    }
  }

  @Override
  public final void onSubscribe(Subscription s) {
    if (Operators.validate(subscription, s)) {
      this.subscription = s;
      try {
        hookOnSubscribe(s);
        requestN();
      } catch (Throwable throwable) {
        onError(Operators.onOperatorError(s, throwable, currentContext()));
      }
    }
  }

  @Override
  public final void onNext(T t) {
    try {
      hookOnNext(t);

      if (prefetch == Integer.MAX_VALUE) {
        return;
      }

      final long l = limit;
      int d = deliveredElements + 1;

      if (d == l) {
        d = 0;
        final long r;
        final Subscription s = subscription;

        if (s == null) {
          return;
        }

        synchronized (this) {
          long er = externalRequested;

          if (er >= l) {
            er -= l;
            // keep pendingToFulfil as is since it is eq to prefetch
            r = l;
          } else {
            pendingToFulfil -= l;
            if (er > 0) {
              r = er;
              er = 0;
              pendingToFulfil += r;
            } else {
              r = 0;
            }
          }

          externalRequested = er;
        }

        if (r > 0) {
          s.request(r);
        }
      }

      deliveredElements = d;
    } catch (Throwable e) {
      onError(e);
    }
  }

  @Override
  public final void onError(Throwable t) {
    Subscription s = S.getAndSet(this, Operators.cancelledSubscription());
    if (s == Operators.cancelledSubscription()) {
      Operators.onErrorDropped(t, this.currentContext());
      return;
    }

    try {
      hookOnError(t);
    } catch (Throwable e) {
      e = Exceptions.addSuppressed(e, t);
      Operators.onErrorDropped(e, currentContext());
    } finally {
      safeHookFinally(SignalType.ON_ERROR);
    }
  }

  @Override
  public final void onComplete() {
    if (S.getAndSet(this, Operators.cancelledSubscription()) != Operators.cancelledSubscription()) {
      // we're sure it has not been concurrently cancelled
      try {
        hookOnComplete();
      } catch (Throwable throwable) {
        // onError itself will short-circuit due to the CancelledSubscription being set above
        hookOnError(Operators.onOperatorError(throwable, currentContext()));
      } finally {
        safeHookFinally(SignalType.ON_COMPLETE);
      }
    }
  }

  @Override
  public final void request(long n) {
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
    final Subscription s = subscription;

    if (s == null) {
      return;
    }

    synchronized (this) {
      final long er = externalRequested;
      final long p = prefetch;
      final int pendingFulfil = pendingToFulfil;

      if (er != Long.MAX_VALUE || p != Integer.MAX_VALUE) {
        // shortcut
        if (pendingFulfil == p) {
          return;
        }

        r = Math.min(p - pendingFulfil, er);
        if (er != Long.MAX_VALUE) {
          externalRequested -= r;
        }
        if (p != Integer.MAX_VALUE) {
          pendingToFulfil += r;
        }
      } else {
        r = Long.MAX_VALUE;
      }
    }

    if (r > 0) {
      s.request(r);
    }
  }

  public final void cancel() {
    if (Operators.terminate(S, this)) {
      try {
        hookOnCancel();
      } catch (Throwable throwable) {
        hookOnError(Operators.onOperatorError(subscription, throwable, currentContext()));
      } finally {
        safeHookFinally(SignalType.CANCEL);
      }
    }
  }
}
