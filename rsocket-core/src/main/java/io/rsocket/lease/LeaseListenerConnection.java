package io.rsocket.lease;

import io.rsocket.DuplexConnection;
import io.rsocket.Frame;
import io.rsocket.FrameType;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;
/** Listens for LEASE frames and exposes them as Lease flux */
public class LeaseListenerConnection implements DuplexConnection {
  private final DuplexConnection duplexConnection;
  private final UnicastProcessor<Lease> leases = UnicastProcessor.create();

  public LeaseListenerConnection(DuplexConnection duplexConnection) {
    this.duplexConnection = duplexConnection;
  }

  @Override
  public Mono<Void> send(Publisher<Frame> frame) {
    return duplexConnection.send(frame);
  }

  public Flux<Lease> leaseReceived() {
    return leases;
  }

  @Override
  public Flux<Frame> receive() {
    return duplexConnection
        .receive()
        .doOnNext(
            frame -> {
              if (frame.getType().equals(FrameType.LEASE)) {
                leases.onNext(new LeaseImpl(frame));
              }
            })
        .doOnTerminate(() -> leases.onComplete());
  }

  @Override
  public double availability() {
    return duplexConnection.availability();
  }

  @Override
  public Mono<Void> close() {
    return duplexConnection.close();
  }

  @Override
  public Mono<Void> onClose() {
    return duplexConnection.onClose();
  }
}
