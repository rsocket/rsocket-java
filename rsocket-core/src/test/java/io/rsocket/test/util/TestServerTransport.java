package io.rsocket.test.util;

import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Closeable;
import io.rsocket.buffer.LeaksTrackingByteBufAllocator;
import io.rsocket.transport.ServerTransport;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

public class TestServerTransport implements ServerTransport<Closeable> {
  private final MonoProcessor<TestDuplexConnection> conn = MonoProcessor.create();
  private final LeaksTrackingByteBufAllocator allocator =
      LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);

  @Override
  public Mono<Closeable> start(ConnectionAcceptor acceptor, int mtu) {
    conn.flatMap(acceptor::apply)
        .subscribe(ignored -> {}, err -> disposeConnection(), this::disposeConnection);
    return Mono.just(
        new Closeable() {
          @Override
          public Mono<Void> onClose() {
            return conn.then();
          }

          @Override
          public void dispose() {
            conn.onComplete();
          }

          @Override
          public boolean isDisposed() {
            return conn.isTerminated();
          }
        });
  }

  private void disposeConnection() {
    TestDuplexConnection c = conn.peek();
    if (c != null) {
      c.dispose();
    }
  }

  public TestDuplexConnection connect() {
    TestDuplexConnection c = new TestDuplexConnection(allocator);
    conn.onNext(c);
    return c;
  }

  public LeaksTrackingByteBufAllocator alloc() {
    return allocator;
  }
}
