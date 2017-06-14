package io.rsocket;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class DuplexConnectionDecorator implements DuplexConnection {
  protected final DuplexConnection delegate;

  public DuplexConnectionDecorator(DuplexConnection delegate) {
    this.delegate = delegate;
  }

  @Override
  public Mono<Void> close() {
    return delegate.close();
  }

  @Override
  public Mono<Void> onClose() {
    return delegate.onClose();
  }

  @Override
  public double availability() {
    return delegate.availability();
  }

  @Override
  public Mono<Void> send(Publisher<Frame> frame) {
    return delegate.send(frame);
  }

  @Override
  public Flux<Frame> receive() {
    return delegate.receive();
  }
}
