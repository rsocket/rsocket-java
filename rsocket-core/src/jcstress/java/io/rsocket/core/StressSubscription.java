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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Operators;

public class StressSubscription<T> implements Subscription {

  CoreSubscriber<? super T> actual;

  public volatile int subscribes;

  @SuppressWarnings("rawtypes")
  static final AtomicIntegerFieldUpdater<StressSubscription> SUBSCRIBES =
      AtomicIntegerFieldUpdater.newUpdater(StressSubscription.class, "subscribes");

  public volatile long requested;

  @SuppressWarnings("rawtypes")
  static final AtomicLongFieldUpdater<StressSubscription> REQUESTED =
      AtomicLongFieldUpdater.newUpdater(StressSubscription.class, "requested");

  public volatile int requestsCount;

  @SuppressWarnings("rawtype  s")
  static final AtomicIntegerFieldUpdater<StressSubscription> REQUESTS_COUNT =
      AtomicIntegerFieldUpdater.newUpdater(StressSubscription.class, "requestsCount");

  public volatile boolean cancelled;

  void subscribe(CoreSubscriber<? super T> actual) {
    this.actual = actual;
    actual.onSubscribe(this);
    SUBSCRIBES.getAndIncrement(this);
  }

  @Override
  public void request(long n) {
    REQUESTS_COUNT.incrementAndGet(this);
    Operators.addCap(REQUESTED, this, n);
  }

  @Override
  public void cancel() {
    cancelled = true;
  }
}
