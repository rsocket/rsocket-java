package io.rsocket.examples.transport.tcp.lease.advanced.controller;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

public class TasksHandlingRSocket implements RSocket {

  private static final Logger logger = LoggerFactory.getLogger(TasksHandlingRSocket.class);

  final Disposable terminatable;
  final Scheduler workScheduler;
  final int processingTime;

  public TasksHandlingRSocket(Disposable terminatable, Scheduler scheduler, int processingTime) {
    this.terminatable = terminatable;
    this.workScheduler = scheduler;
    this.processingTime = processingTime;
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {

    // specifically to show that lease can limit rate of fnf requests in
    // that example
    String message = payload.getDataUtf8();
    payload.release();

    return Mono.<Void>fromRunnable(new Task(message, processingTime))
        // schedule task on specific, limited in size scheduler
        .subscribeOn(workScheduler)
        // if errors - terminates server
        .doOnError(
            t -> {
              logger.error("Queue has been overflowed. Terminating server");
              terminatable.dispose();
            });
  }
}
