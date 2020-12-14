package io.rsocket.examples.transport.tcp.lease.advanced.common;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class LeaseManager implements Runnable {

  static final Logger logger = LoggerFactory.getLogger(LeaseManager.class);

  volatile int activeConnectionsCount;
  static final AtomicIntegerFieldUpdater<LeaseManager> ACTIVE_CONNECTIONS_COUNT =
      AtomicIntegerFieldUpdater.newUpdater(LeaseManager.class, "activeConnectionsCount");

  volatile int stateAndInFlight;
  static final AtomicIntegerFieldUpdater<LeaseManager> STATE_AND_IN_FLIGHT =
      AtomicIntegerFieldUpdater.newUpdater(LeaseManager.class, "stateAndInFlight");

  static final int MASK_PAUSED = 0b1_000_0000_0000_0000_0000_0000_0000_0000;
  static final int MASK_IN_FLIGHT = 0b0_111_1111_1111_1111_1111_1111_1111_1111;

  final BlockingDeque<LimitBasedLeaseSender> sendersQueue = new LinkedBlockingDeque<>();
  final Scheduler worker = Schedulers.newSingle(LeaseManager.class.getName());

  final int capacity;
  final int ttl;

  public LeaseManager(int capacity, int ttl) {
    this.capacity = capacity;
    this.ttl = ttl;
  }

  @Override
  public void run() {
    try {
      LimitBasedLeaseSender leaseSender = sendersQueue.poll();

      if (leaseSender == null) {
        return;
      }

      if (leaseSender.isDisposed()) {
        logger.debug("Connection[" + leaseSender.connectionId + "]: LeaseSender is Disposed");
        worker.schedule(this);
        return;
      }

      int limit = leaseSender.limitAlgorithm.getLimit();

      if (limit == 0) {
        throw new IllegalStateException("Limit is 0");
      }

      if (pauseIfNoCapacity()) {
        sendersQueue.addFirst(leaseSender);
        logger.debug("Pause execution. Not enough capacity");
        return;
      }

      leaseSender.sendLease(ttl, limit);
      sendersQueue.offer(leaseSender);

      int activeConnections = activeConnectionsCount;
      int nextDelay = activeConnections == 0 ? ttl : (ttl / activeConnections);

      logger.debug("Next check happens in " + nextDelay + "ms");

      worker.schedule(this, nextDelay, TimeUnit.MILLISECONDS);
    } catch (Throwable e) {
      logger.error("LeaseSender failed to send lease", e);
    }
  }

  int incrementInFlightAndGet() {
    for (; ; ) {
      int state = stateAndInFlight;
      int paused = state & MASK_PAUSED;
      int inFlight = stateAndInFlight & MASK_IN_FLIGHT;

      // assume overflow is impossible due to max concurrency in RSocket it self
      int nextInFlight = inFlight + 1;

      if (STATE_AND_IN_FLIGHT.compareAndSet(this, state, nextInFlight | paused)) {
        return nextInFlight;
      }
    }
  }

  void decrementInFlight() {
    for (; ; ) {
      int state = stateAndInFlight;
      int paused = state & MASK_PAUSED;
      int inFlight = stateAndInFlight & MASK_IN_FLIGHT;

      // assume overflow is impossible due to max concurrency in RSocket it self
      int nextInFlight = inFlight - 1;

      if (inFlight == capacity && paused == MASK_PAUSED) {
        if (STATE_AND_IN_FLIGHT.compareAndSet(this, state, nextInFlight)) {
          logger.debug("Resume execution");
          worker.schedule(this);
          return;
        }
      } else {
        if (STATE_AND_IN_FLIGHT.compareAndSet(this, state, nextInFlight | paused)) {
          return;
        }
      }
    }
  }

  boolean pauseIfNoCapacity() {
    int capacity = this.capacity;
    for (; ; ) {
      int inFlight = stateAndInFlight;

      if (inFlight < capacity) {
        return false;
      }

      if (STATE_AND_IN_FLIGHT.compareAndSet(this, inFlight, inFlight | MASK_PAUSED)) {
        return true;
      }
    }
  }

  void unregister() {
    ACTIVE_CONNECTIONS_COUNT.decrementAndGet(this);
  }

  void register(LimitBasedLeaseSender sender) {
    sendersQueue.offer(sender);
    final int activeCount = ACTIVE_CONNECTIONS_COUNT.getAndIncrement(this);

    if (activeCount == 0) {
      worker.schedule(this);
    }
  }
}
