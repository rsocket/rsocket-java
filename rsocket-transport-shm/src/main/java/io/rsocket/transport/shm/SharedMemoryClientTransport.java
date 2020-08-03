package io.rsocket.transport.shm;

import io.netty.buffer.ByteBufAllocator;
import io.rsocket.DuplexConnection;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.shm.io.Socket;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

public class SharedMemoryClientTransport implements ClientTransport {

  public static SharedMemoryClientTransport create(int port) {
    return create(port, ByteBufAllocator.DEFAULT, Schedulers.parallel());
  }

  public static SharedMemoryClientTransport create(int port, Scheduler eventLoopGroup) {
    return create(port, ByteBufAllocator.DEFAULT, eventLoopGroup);
  }

  public static SharedMemoryClientTransport create(int port, ByteBufAllocator alloc) {
    return create(port, alloc, Schedulers.parallel());
  }

  public static SharedMemoryClientTransport create(
      int port, ByteBufAllocator alloc, Scheduler eventLoopGroup) {
    return new SharedMemoryClientTransport(port, alloc, eventLoopGroup);
  }

  final int port;
  final ByteBufAllocator alloc;
  final Scheduler eventLoopGroup;

  SharedMemoryClientTransport(int port, ByteBufAllocator alloc, Scheduler eventLoopGroup) {
    this.port = port;
    this.alloc = alloc;
    this.eventLoopGroup = eventLoopGroup;
  }

  @Override
  public Mono<DuplexConnection> connect() {
    return Mono.<DuplexConnection>fromCallable(
            () -> {
              Socket socket = new Socket(port);

              return new SharedMemoryDuplexConnection(
                  eventLoopGroup, alloc, socket, Queues.XS_BUFFER_SIZE);
            })
        .subscribeOn(Schedulers.boundedElastic());
  }
}
