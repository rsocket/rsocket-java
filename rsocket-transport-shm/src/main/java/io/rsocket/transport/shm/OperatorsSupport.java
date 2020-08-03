package io.rsocket.transport.shm;

import io.netty.buffer.ByteBuf;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import reactor.core.publisher.Operators;
import reactor.util.context.Context;

final class OperatorsSupport {

  static <T> int wipIncrement(AtomicIntegerFieldUpdater<T> updater, T instance) {
    for (; ; ) {
      int wip = updater.get(instance);

      if (wip == Integer.MIN_VALUE) {
        return Integer.MIN_VALUE;
      }

      if (updater.compareAndSet(instance, wip, wip + 1)) {
        return wip;
      }
    }
  }

  static <T> void discardWithTermination(
      AtomicIntegerFieldUpdater<T> updater, T instance, Queue<ByteBuf> q, Context context) {

    for (; ; ) {
      int wip = updater.get(instance);

      // In all other modes we are free to discard queue immediately
      // since there is no racing on polling
      Operators.onDiscardQueueWithClear(q, context, null);

      if (updater.compareAndSet(instance, wip, Integer.MIN_VALUE)) {
        break;
      }
    }
  }

  static <T> void discardAsyncWithTermination(
      AtomicIntegerFieldUpdater<T> updater, T instance, Queue<ByteBuf> q) {

    for (; ; ) {
      int wip = updater.get(instance);

      // delegates discarding to the queue holder to ensure there is no racing on draining from the
      // SpScQueue
      q.clear();

      if (updater.compareAndSet(instance, wip, Integer.MIN_VALUE)) {
        break;
      }
    }
  }

  static <T> long addCapCancellable(AtomicLongFieldUpdater<T> updater, T instance, long n) {
    for (; ; ) {
      long r = updater.get(instance);
      if (r == Long.MIN_VALUE || r == Long.MAX_VALUE) {
        return r;
      }
      long u = Operators.addCap(r, n);
      if (updater.compareAndSet(instance, r, u)) {
        return r;
      }
    }
  }
}
