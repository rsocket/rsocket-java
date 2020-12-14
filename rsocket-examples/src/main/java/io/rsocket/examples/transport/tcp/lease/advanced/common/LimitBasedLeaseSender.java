package io.rsocket.examples.transport.tcp.lease.advanced.common;

import com.netflix.concurrency.limits.Limit;
import io.rsocket.lease.Lease;
import io.rsocket.lease.LeaseSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.util.concurrent.Queues;

public class LimitBasedLeaseSender extends LimitBasedStatsCollector implements LeaseSender {

  static final Logger logger = LoggerFactory.getLogger(LimitBasedLeaseSender.class);

  final String connectionId;
  final Sinks.Many<Lease> sink =
      Sinks.many().unicast().onBackpressureBuffer(Queues.<Lease>one().get());

  public LimitBasedLeaseSender(
      String connectionId, LeaseManager leaseManager, Limit limitAlgorithm) {
    super(leaseManager, limitAlgorithm);
    this.connectionId = connectionId;
  }

  @Override
  public Flux<Lease> send() {
    logger.info("Received new leased Connection[" + connectionId + "]");

    leaseManager.register(this);

    return sink.asFlux();
  }

  public void sendLease(int ttl, int amount) {
    final Lease nextLease = Lease.create(ttl, amount);
    final Sinks.EmitResult result = sink.tryEmitNext(nextLease);

    if (result.isFailure()) {
      logger.warn(
          "Connection["
              + connectionId
              + "]. Issued Lease: ["
              + nextLease
              + "] was not sent due to "
              + result);
    } else {
      if (logger.isDebugEnabled()) {
        logger.debug("To Connection[" + connectionId + "]: Issued Lease: [" + nextLease + "]");
      }
    }
  }
}
