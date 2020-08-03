package io.rsocket.transport.shm;

import io.netty.buffer.ByteBufAllocator;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.util.DefaultPayload;
import java.time.Duration;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class TestClient {

  public static void main(String[] args) {
    Scheduler eventLoopGroup = Schedulers.newParallel("shm-event-loop");
    RSocket rSocket =
        RSocketConnector.create()
            .keepAlive(Duration.ofMillis(Integer.MAX_VALUE), Duration.ofMillis(Integer.MAX_VALUE))
            .connect(
                new SharedMemoryClientTransport(8081, ByteBufAllocator.DEFAULT, eventLoopGroup))
            .block();

    System.out.println("Connected ");

    rSocket
        .requestResponse(DefaultPayload.create("test"))
        .map(
            p -> {
              String content = p.getDataUtf8();
              p.release();
              return content;
            })
        .log()
        .block();

    rSocket
        .requestStream(DefaultPayload.create("test2"))
        .map(
            p -> {
              String content = p.getDataUtf8();
              p.release();
              return content;
            })
        .log()
        .blockLast();
  }
}
