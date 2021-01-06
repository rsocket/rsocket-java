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
