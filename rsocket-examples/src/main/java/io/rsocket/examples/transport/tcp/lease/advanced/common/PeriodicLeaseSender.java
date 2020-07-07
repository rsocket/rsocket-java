package io.rsocket.examples.transport.tcp.lease.advanced.common;

import io.rsocket.lease.Lease;
import io.rsocket.lease.LeaseSender;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;

public class PeriodicLeaseSender implements Runnable, LeaseSender<LimitBasedRequestTracker> {

  static final Logger logger = LoggerFactory.getLogger(PeriodicLeaseSender.class);

  final ConcurrentMap<LimitBasedRequestTracker, FluxProcessor<Lease, Lease>>
      connectionsStatsToFluxSenders;
  final BlockingDeque<LimitBasedRequestTracker> connectionsQueue;
  final Scheduler.Worker worker;
  final int capacity;
  final int ttl;

  volatile int activeConnectionsCount;
  static final AtomicIntegerFieldUpdater<PeriodicLeaseSender> ACTIVE_CONNECTIONS_COUNT =
      AtomicIntegerFieldUpdater.newUpdater(PeriodicLeaseSender.class, "activeConnectionsCount");

  volatile int stateAndInFlight;
  static final AtomicIntegerFieldUpdater<PeriodicLeaseSender> STATE_AND_IN_FLIGHT =
      AtomicIntegerFieldUpdater.newUpdater(PeriodicLeaseSender.class, "stateAndInFlight");

  static final int MASK_PAUSED = 0b1_000_0000_0000_0000_0000_0000_0000_0000;
  static final int MASK_IN_FLIGHT = 0b0_111_1111_1111_1111_1111_1111_1111_1111;

  public PeriodicLeaseSender(int capacity, long ttl, Scheduler.Worker worker) {
    this.worker = worker;
    this.connectionsStatsToFluxSenders = new ConcurrentHashMap<>();
    this.connectionsQueue = new LinkedBlockingDeque<>();

    if (ttl == 0) {
      this.ttl = Integer.MAX_VALUE;
      this.capacity = Integer.MAX_VALUE;
    } else {
      this.capacity = capacity;
      this.ttl = (int) ttl;
    }
  }

  @Override
  public Flux<Lease> send(@Nullable LimitBasedRequestTracker limitBasedLeaseTracker) {
    if (limitBasedLeaseTracker == null) {
      return Flux.error(new IllegalStateException("LeaseTracker must be present"));
    }

    UnicastProcessor<Lease> leaseUnicastFluxProcessor =
        UnicastProcessor.create(Queues.<Lease>one().get());

    logger.info("Received new leased Connection[" + limitBasedLeaseTracker.connectionId + "]");

    if (capacity == Integer.MAX_VALUE) {
      logger.info(
          "To Connection[" + limitBasedLeaseTracker.connectionId + "]: Issued Unbounded Lease");
      leaseUnicastFluxProcessor.onNext(Lease.create(ttl, capacity));
    } else {
      connectionsStatsToFluxSenders.put(limitBasedLeaseTracker, leaseUnicastFluxProcessor);
      int currentActive = ACTIVE_CONNECTIONS_COUNT.getAndIncrement(this);
      connectionsQueue.offer(limitBasedLeaseTracker);

      if (currentActive == 0) {
        worker.schedule(this);
      }
    }

    return leaseUnicastFluxProcessor;
  }

  @Override
  public void run() {
    try {
      LimitBasedRequestTracker leaseStats = connectionsQueue.poll();

      if (leaseStats == null) {
        return;
      }

      FluxProcessor<Lease, Lease> leaseSender = connectionsStatsToFluxSenders.get(leaseStats);

      if (leaseSender == null || leaseSender.isDisposed()) {
        logger.debug("Connection[" + leaseStats.connectionId + "]: LeaseSender is Disposed");
        worker.schedule(this);
        return;
      }

      int limit = leaseStats.limitAlgorithm.getLimit();

      if (limit == 0) {
        throw new IllegalStateException("Limit is 0");
      }

      if (pauseIfNoCapacity()) {
        connectionsQueue.addFirst(leaseStats);
        logger.debug("Pause execution. Not enough capacity");
        return;
      }

      Lease nextLease = Lease.create(ttl, limit);

      logger.debug(
          "To Connection[" + leaseStats.connectionId + "]: Issued Lease: [" + nextLease + "]");

      leaseSender.onNext(nextLease);

      connectionsQueue.offer(leaseStats);

      int activeConnections = connectionsStatsToFluxSenders.size();
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

  void remove(LimitBasedRequestTracker stats) {
    connectionsStatsToFluxSenders.remove(stats);
    ACTIVE_CONNECTIONS_COUNT.decrementAndGet(this);
  }
}
