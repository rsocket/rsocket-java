/*
 * Copyright 2015-present the original author or authors.
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
package io.rsocket.transport.aeron;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

class Operators {

  static <T> long addCapCancellable(AtomicLongFieldUpdater<T> updater, T instance, long n) {
    for (; ; ) {
      long r = updater.get(instance);
      if (r == Long.MIN_VALUE || r == Long.MAX_VALUE) {
        return r;
      }
      long u = addCap(r, n);
      if (updater.compareAndSet(instance, r, u)) {
        return r;
      }
    }
  }

  static <T> long removeCapCancellable(AtomicLongFieldUpdater<T> updater, T instance, long n) {
    for (; ; ) {
      long r = updater.get(instance);
      if (r == Long.MIN_VALUE || r == Long.MAX_VALUE) {
        return r;
      }
      long u = addCap(r, -n);
      if (updater.compareAndSet(instance, r, u)) {
        return u;
      }
    }
  }

  static long addCap(long a, long b) {
    long res = a + b;
    if (res < 0L) {
      return Long.MAX_VALUE;
    }
    return res;
  }
}
