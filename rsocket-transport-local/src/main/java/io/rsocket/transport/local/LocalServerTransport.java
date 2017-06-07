package io.rsocket.transport.local;

import io.rsocket.Closeable;
import io.rsocket.DuplexConnection;
import io.rsocket.transport.ServerTransport;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

public class LocalServerTransport implements ServerTransport<Closeable> {
  private static final ConcurrentMap<String, ServerDuplexConnectionAcceptor> registry =
      new ConcurrentHashMap<>();

  static ServerDuplexConnectionAcceptor findServer(String name) {
    return registry.get(name);
  }

  private final String name;

  private LocalServerTransport(String name) {
    this.name = name;
  }

  public static LocalServerTransport create(String name) {
    return new LocalServerTransport(name);
  }

  @Override
  public Mono<Closeable> start(ConnectionAcceptor acceptor) {
    return Mono.create(
        sink -> {
          ServerDuplexConnectionAcceptor serverDuplexConnectionAcceptor =
              new ServerDuplexConnectionAcceptor(name, acceptor);
          if (registry.putIfAbsent(name, serverDuplexConnectionAcceptor) != null) {
            throw new IllegalStateException("name already registered: " + name);
          }
          sink.success(serverDuplexConnectionAcceptor);
        });
  }

  static class ServerDuplexConnectionAcceptor implements Consumer<DuplexConnection>, Closeable {
    private final LocalSocketAddress address;
    private final ConnectionAcceptor acceptor;
    private final MonoProcessor<Void> closeNotifier = MonoProcessor.create();

    public ServerDuplexConnectionAcceptor(String name, ConnectionAcceptor acceptor) {
      this.address = new LocalSocketAddress(name);
      this.acceptor = acceptor;
    }

    @Override
    public void accept(DuplexConnection duplexConnection) {
      acceptor.apply(duplexConnection).subscribe();
    }

    @Override
    public Mono<Void> close() {
      return Mono.defer(
          () -> {
            if (!registry.remove(address.getName(), this)) {
              throw new AssertionError();
            }

            closeNotifier.onComplete();
            return Mono.empty();
          });
    }

    @Override
    public Mono<Void> onClose() {
      return closeNotifier;
    }
  }
}
