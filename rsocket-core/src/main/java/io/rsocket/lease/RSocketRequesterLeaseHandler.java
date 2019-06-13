package io.rsocket.lease;

import io.netty.buffer.ByteBuf;
import io.rsocket.exceptions.MissingLeaseException;
import io.rsocket.frame.LeaseFrameFlyweight;
import java.util.function.Consumer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.ReplayProcessor;

public class RSocketRequesterLeaseHandler implements RequesterLeaseHandler {
  private final String tag;
  private final ReplayProcessor<Lease> receivedLease;
  private volatile LeaseImpl currentLease = LeaseImpl.empty();

  public RSocketRequesterLeaseHandler(String tag, Consumer<Flux<Lease>> leaseReceiver) {
    this.tag = tag;
    receivedLease = ReplayProcessor.create(1);
    leaseReceiver.accept(receivedLease);
  }

  @Override
  public boolean useLease() {
    return currentLease.use();
  }

  @Override
  public Exception leaseError() {
    LeaseImpl l = this.currentLease;
    String t = this.tag;
    if (!l.isValid()) {
      return new MissingLeaseException(l, t);
    } else {
      return new MissingLeaseException(t);
    }
  }

  @Override
  public void receive(ByteBuf leaseFrame) {
    int numberOfRequests = LeaseFrameFlyweight.numRequests(leaseFrame);
    int timeToLiveMillis = LeaseFrameFlyweight.ttl(leaseFrame);
    ByteBuf metadata = LeaseFrameFlyweight.metadata(leaseFrame);
    LeaseImpl lease = LeaseImpl.create(timeToLiveMillis, numberOfRequests, metadata);
    currentLease = lease;
    receivedLease.onNext(lease);
  }

  @Override
  public void dispose() {
    receivedLease.onComplete();
  }

  @Override
  public double availability() {
    return currentLease.availability();
  }
}
