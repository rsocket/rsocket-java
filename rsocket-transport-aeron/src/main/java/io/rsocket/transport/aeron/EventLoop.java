package io.rsocket.transport.aeron;

import io.rsocket.internal.jctools.queues.MpscUnboundedArrayQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.concurrent.IdleStrategy;
import reactor.core.Disposable;

public final class EventLoop extends AtomicBoolean implements Runnable, Disposable {

  final MpscUnboundedArrayQueue<Runnable> tasksQueue;
  final IdleStrategy idleStrategy;

  boolean terminated = false;

  EventLoop(IdleStrategy idleStrategy) {
    this.idleStrategy = idleStrategy;
    this.tasksQueue = new MpscUnboundedArrayQueue<>(256);
  }

  @Override
  public void dispose() {
    if (!compareAndSet(false, true)) {
      return;
    }

    this.tasksQueue.offer(() -> this.terminated = true);
  }

  @Override
  public boolean isDisposed() {
    return get();
  }

  @Override
  public void run() {
    final MpscUnboundedArrayQueue<Runnable> tasksQueue = this.tasksQueue;
    final IdleStrategy idleStrategy = this.idleStrategy;

    while (!this.terminated) {
      Runnable task = tasksQueue.relaxedPoll();

      if (task == null) {
        idleStrategy.reset();
        while (task == null) {
          if (this.terminated) {
            return;
          }

          idleStrategy.idle();
          task = tasksQueue.relaxedPoll();
        }
      }

      task.run();
    }
  }

  void schedule(Runnable task) {
    this.tasksQueue.offer(task);
  }
}
