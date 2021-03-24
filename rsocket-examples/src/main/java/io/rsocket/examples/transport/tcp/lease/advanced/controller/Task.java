package io.rsocket.examples.transport.tcp.lease.advanced.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// emulating a worker that process data from the queue
public class Task implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(Task.class);

  final String message;
  final int processingTime;

  Task(String message, int processingTime) {
    this.message = message;
    this.processingTime = processingTime;
  }

  @Override
  public void run() {
    logger.info("Processing Task[{}]", message);
    try {
      Thread.sleep(processingTime); // emulating processing
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
