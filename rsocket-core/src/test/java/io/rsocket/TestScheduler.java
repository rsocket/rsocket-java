package io.rsocket;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Exceptions;
import reactor.core.scheduler.Scheduler;
import reactor.util.concurrent.Queues;

/**
 * This is an implementation of scheduler which allows task execution on the caller thread or
 * scheduling it for thread which are currently working (with "work stealing" behaviour)
 */
public final class TestScheduler implements Scheduler {

  public static final Scheduler INSTANCE = new TestScheduler();

  volatile int wip;
  static final AtomicIntegerFieldUpdater<TestScheduler> WIP =
      AtomicIntegerFieldUpdater.newUpdater(TestScheduler.class, "wip");

  final Worker sharedWorker = new TestWorker(this);
  final Queue<Runnable> tasks = Queues.<Runnable>unboundedMultiproducer().get();

  private TestScheduler() {}

  @Override
  public Disposable schedule(Runnable task) {
    tasks.offer(task);
    if (WIP.getAndIncrement(this) != 0) {
      return Disposables.never();
    }

    int missed = 1;

    for (; ; ) {
      for (; ; ) {
        Runnable runnable = tasks.poll();

        if (runnable == null) {
          break;
        }

        try {
          runnable.run();
        } catch (Throwable t) {
          Exceptions.throwIfFatal(t);
        }
      }

      missed = WIP.addAndGet(this, -missed);
      if (missed == 0) {
        return Disposables.never();
      }
    }
  }

  @Override
  public Worker createWorker() {
    return sharedWorker;
  }

  static class TestWorker implements Worker {

    final TestScheduler parent;

    TestWorker(TestScheduler parent) {
      this.parent = parent;
    }

    @Override
    public Disposable schedule(Runnable task) {
      return parent.schedule(task);
    }

    @Override
    public void dispose() {}
  }
}
