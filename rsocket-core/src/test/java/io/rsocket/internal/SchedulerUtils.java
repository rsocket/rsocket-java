package io.rsocket.internal;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import reactor.core.scheduler.Scheduler;

public class SchedulerUtils {

  public static void warmup(Scheduler scheduler) throws InterruptedException {
    warmup(scheduler, 10000);
  }

  public static void warmup(Scheduler scheduler, int times) throws InterruptedException {
    scheduler.start();

    // warm up
    CountDownLatch latch = new CountDownLatch(times);
    for (int i = 0; i < times; i++) {
      scheduler.schedule(latch::countDown);
    }
    latch.await(5, TimeUnit.SECONDS);
  }
}
