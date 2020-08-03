package io.rsocket.transport.shm;

import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Closeable;
import io.rsocket.DuplexConnection;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.shm.io.Server;
import io.rsocket.transport.shm.io.Socket;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.Operators;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

public class SharedMemoryServerTransport implements ServerTransport<Closeable> {

  public static SharedMemoryServerTransport create(int port) {
    return create(port, ByteBufAllocator.DEFAULT, Schedulers.parallel());
  }

  public static SharedMemoryServerTransport create(int port, Scheduler eventLoopGroup) {
    return create(port, ByteBufAllocator.DEFAULT, eventLoopGroup);
  }

  public static SharedMemoryServerTransport create(int port, ByteBufAllocator alloc) {
    return create(port, alloc, Schedulers.parallel());
  }

  public static SharedMemoryServerTransport create(
      int port, ByteBufAllocator alloc, Scheduler eventLoopGroup) {
    return new SharedMemoryServerTransport(port, alloc, eventLoopGroup);
  }

  final int port;
  final ByteBufAllocator alloc;
  final Scheduler eventLoopGroup;

  SharedMemoryServerTransport(int port, ByteBufAllocator alloc, Scheduler eventLoopGroup) {
    this.port = port;
    this.alloc = alloc;
    this.eventLoopGroup = eventLoopGroup;
  }

  @Override
  public Mono<Closeable> start(ConnectionAcceptor acceptor) {
    return Flux.create(
            (sink) -> {
              List<DuplexConnection> connections = new CopyOnWriteArrayList<>();
              Server server = null;
              try {
                server = new Server(port);
              } catch (IOException e) {
                sink.error(e);
                return;
              }

              while (!sink.isCancelled()) {
                try {
                  Socket socket = server.accept();
                  DuplexConnection connection =
                      new SharedMemoryDuplexConnection(
                          eventLoopGroup, alloc, socket, Queues.XS_BUFFER_SIZE);
                  connections.add(connection);
                  connection
                      .onClose()
                      .subscribe(
                          null,
                          t -> connections.remove(connection),
                          () -> connections.remove(connection));
                  acceptor.apply(connection).subscribe();
                } catch (IOException e) {
                  sink.error(e);
                }
              }

              for (DuplexConnection connection : connections) {
                connection.dispose();
              }
            })
        .subscribeOn(Schedulers.elastic())
        .then()
        .<Closeable>transform(Operators.lift((__, a) -> new CloseableServer(a)));
  }

  static class CloseableServer extends Operators.MonoSubscriber<Void, Closeable>
      implements Closeable {

    final MonoProcessor<Void> onClose = MonoProcessor.create();

    Subscription s;

    CloseableServer(CoreSubscriber<? super Closeable> actual) {
      super(actual);
    }

    @Override
    public void onSubscribe(Subscription s) {
      if (Operators.validate(this.s, s)) {
        this.s = s;
        s.request(Long.MAX_VALUE);
        actual.onSubscribe(this);
        complete(this);
      }
    }

    @Override
    public void cancel() {
      super.cancel();
      s.cancel();
      onClose.onComplete();
    }

    @Override
    public void onError(Throwable throwable) {
      onClose.onError(throwable);
    }

    @Override
    public void onComplete() {
      onClose.onComplete();
    }

    @Override
    public Mono<Void> onClose() {
      return onClose;
    }

    @Override
    public void dispose() {
      cancel();
    }
  }
}
