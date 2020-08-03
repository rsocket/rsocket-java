package io.rsocket.transport.shm;

import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Closeable;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketServer;
import io.rsocket.util.DefaultPayload;
import java.util.logging.Level;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class TestServer {

  public static void main(String[] args) {
    Scheduler eventLoopGroup = Schedulers.newParallel("shm-event-loop");
    Closeable closeable =
        RSocketServer.create(
                SocketAcceptor.with(
                    new RSocket() {
                      @Override
                      public Mono<Payload> requestResponse(Payload p) {
                        return Mono.just(DefaultPayload.create("Echo:" + p.getDataUtf8()))
                            .log()
                            .doFinally(__ -> p.release());
                      }

                      @Override
                      public Flux<Payload> requestStream(Payload payload) {
                        return Flux.range(0, Integer.MAX_VALUE)
                            .map(
                                i ->
                                    DefaultPayload.create(
                                        "Echo[" + i + "]:" + payload.getDataUtf8()))
                            .log("server", Level.INFO, SignalType.ON_NEXT)
                            .repeat()
                            .doFinally(__ -> payload.release());
                      }
                    }))
            .bindNow(
                new SharedMemoryServerTransport(8081, ByteBufAllocator.DEFAULT, eventLoopGroup));
    System.out.println("Server started");
    closeable.onClose().block();
  }
}
