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

package io.rsocket.aeron.internal;

import java.util.concurrent.locks.LockSupport;
import java.util.function.IntSupplier;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class SingleThreadedEventLoop implements EventLoop {
  private static final Logger logger = LoggerFactory.getLogger(SingleThreadedEventLoop.class);
  private final String name;
  private final Thread thread;
  private final OneToOneConcurrentArrayQueue<IntSupplier> events =
      new OneToOneConcurrentArrayQueue<>(32768);

  public SingleThreadedEventLoop(String name) {
    this.name = name;
    logger.info("Starting event loop named => {}", name);

    thread = new Thread(new SingleThreadedEventLoopRunnable());
    thread.setDaemon(true);
    thread.setName("aeron-single-threaded-event-loop-" + name);
    thread.start();
  }

  @Override
  public boolean execute(IntSupplier r) {
    boolean offer;

    if (thread == Thread.currentThread()) {
      offer = events.offer(r);
    } else {
      synchronized (this) {
        offer = events.offer(r);
      }
      LockSupport.unpark(thread);
    }

    return offer;
  }

  private int drain() {
    int count = 0;
    while (!events.isEmpty()) {
      IntSupplier poll = events.poll();
      if (poll != null) {
        count += poll.getAsInt();
      }
    }

    return count;
  }

  private class SingleThreadedEventLoopRunnable implements Runnable {
    final IdleStrategy idleStrategy = Constants.EVENT_LOOP_IDLE_STRATEGY;

    @Override
    public void run() {
      while (true) {
        try {
          int count = drain();
          // if (count > 100) {
          //    System.out.println(name + " drained..." + count);
          // }
          idleStrategy.idle(count);
        } catch (Throwable t) {
          System.err.println("Something bad happened - an error made it to the event loop");
          t.printStackTrace();
        }
      }
    }
  }

  @Override
  public String toString() {
    return "SingleThreadedEventLoop{" + "name='" + name + '\'' + '}';
  }
}
