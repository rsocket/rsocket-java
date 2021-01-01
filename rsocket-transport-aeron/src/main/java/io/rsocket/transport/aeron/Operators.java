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
