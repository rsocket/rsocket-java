package io.rsocket.examples.transport.tcp.lease.advanced.common;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.util.RSocketProxy;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class LeaseWaitingRSocket extends RSocketProxy {

  final DeferringLeaseReceiver deferringLeaseReceiver;

  public LeaseWaitingRSocket(RSocket source, DeferringLeaseReceiver deferringLeaseReceiver) {
    super(source);
    this.deferringLeaseReceiver = deferringLeaseReceiver;
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    return deferringLeaseReceiver.acquireLease(super.fireAndForget(payload));
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    return deferringLeaseReceiver.acquireLease(super.requestResponse(payload));
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    return deferringLeaseReceiver.acquireLease(super.requestStream(payload));
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return deferringLeaseReceiver.acquireLease(super.requestChannel(payloads));
  }
}
