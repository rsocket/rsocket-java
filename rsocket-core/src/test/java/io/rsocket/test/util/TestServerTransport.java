package io.rsocket.test.util;

import io.rsocket.Closeable;
import io.rsocket.transport.ServerTransport;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

public class TestServerTransport implements ServerTransport<Closeable> {
  private final MonoProcessor<TestDuplexConnection> conn = MonoProcessor.create();

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
    TestDuplexConnection c = new TestDuplexConnection();
    conn.onNext(c);
    return c;
  }
}
