package io.rsocket.examples.transport.tcp.lease.advanced.common;

import io.rsocket.Payload;
import io.rsocket.lease.Lease;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;
import reactor.core.publisher.SignalType;

public class DefaultDeferringLeaseReceiver extends BaseSubscriber<Lease>
    implements DeferringLeaseReceiver {

  static final Logger logger = LoggerFactory.getLogger(DefaultDeferringLeaseReceiver.class);

  final String id;
  final ReplayProcessor<Lease> lastLeaseReplay;

  volatile int allowedRequests;
  static final AtomicIntegerFieldUpdater<DefaultDeferringLeaseReceiver> ALLOWED_REQUESTS =
      AtomicIntegerFieldUpdater.newUpdater(DefaultDeferringLeaseReceiver.class, "allowedRequests");

  Lease currentLease;

  public DefaultDeferringLeaseReceiver(String id) {
    this.id = id;
    this.lastLeaseReplay = ReplayProcessor.cacheLast();
  }

  @Override
  public void receive(Flux<Lease> receivedLeases) {
    receivedLeases.subscribe(this);
  }

  @Override
  protected void hookOnNext(Lease l) {
    currentLease = l;
    logger.debug(
        "From Connection[" + id + "]: Received leases - ttl: {}, requests: {}",
        l.getTimeToLiveMillis(),
        l.getAllowedRequests());
    ALLOWED_REQUESTS.getAndSet(this, l.getAllowedRequests());
    lastLeaseReplay.onNext(l);
  }

  @Override
  protected void hookFinally(SignalType type) {
    lastLeaseReplay.dispose();
  }

  public Flux<Payload> acquireLease(Flux<Payload> source) {
    return lastLeaseReplay
        .filter(l -> ALLOWED_REQUESTS.getAndDecrement(this) > 0 && l.isValid())
        .next()
        .flatMapMany(l -> source);
  }

  public <T> Mono<T> acquireLease(Mono<T> source) {
    return lastLeaseReplay
        .filter(l -> ALLOWED_REQUESTS.getAndDecrement(this) > 0 && l.isValid())
        .next()
        .flatMap(l -> source);
  }
}
