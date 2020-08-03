package io.rsocket.transport.shm;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.DuplexConnection;
import io.rsocket.transport.shm.io.Socket;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.scheduler.Scheduler;

public class SharedMemoryDuplexConnection implements DuplexConnection {

  final Scheduler eventLoopGroup;
  final ByteBufAllocator alloc;
  final MonoProcessor<Void> onClose;
  final Socket socket;
  final int prefetch;

  public SharedMemoryDuplexConnection(
      Scheduler eventLoopGroup, ByteBufAllocator alloc, Socket socket, int prefetch) {

    this.eventLoopGroup = eventLoopGroup;
    this.alloc = alloc;
    this.socket = socket;
    this.prefetch = prefetch;
    this.onClose = MonoProcessor.create();
  }

  @Override
  public void dispose() {}

  @Override
  public Mono<Void> onClose() {
    return onClose;
  }

  @Override
  public Mono<Void> send(Publisher<ByteBuf> publisher) {
    return new MonoSendMany(socket.getWriter(), publisher, eventLoopGroup.createWorker(), prefetch);
  }

  @Override
  public Flux<ByteBuf> receive() {
    return new FluxReceive(socket.getReader(), alloc, eventLoopGroup.createWorker());
  }

  @Override
  public ByteBufAllocator alloc() {
    return alloc;
  }
}
