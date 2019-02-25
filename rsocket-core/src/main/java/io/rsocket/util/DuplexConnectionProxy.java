package io.rsocket.util;

import io.netty.buffer.ByteBuf;
import io.rsocket.DuplexConnection;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class DuplexConnectionProxy implements DuplexConnection {
  private final DuplexConnection connection;

  public DuplexConnectionProxy(DuplexConnection connection) {
    this.connection = connection;
  }

  @Override
  public Mono<Void> send(Publisher<ByteBuf> frames) {
    return connection.send(frames);
  }

  @Override
  public Flux<ByteBuf> receive() {
    return connection.receive();
  }

  @Override
  public double availability() {
    return connection.availability();
  }

  @Override
  public Mono<Void> onClose() {
    return connection.onClose();
  }

  @Override
  public void dispose() {
    connection.dispose();
  }

  @Override
  public boolean isDisposed() {
    return connection.isDisposed();
  }

  public DuplexConnection delegate() {
    return connection;
  }
}
