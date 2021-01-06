/*
 * Copyright 2015-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.transport.aeron;

import static java.util.concurrent.TimeUnit.SECONDS;

import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Closeable;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketServer;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.util.ByteBufPayload;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.concurrent.ThreadLocalRandom;
import org.agrona.concurrent.BackoffIdleStrategy;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public final class AeronPongServer {
  static {
    final MediaDriver.Context ctx =
        new MediaDriver.Context().threadingMode(ThreadingMode.DEDICATED).dirDeleteOnStart(true);
    MediaDriver.launch(ctx);
  }

  public static void main(String... args) {
    String aeronUrl = "aeron:ipc";
    Aeron aeron = Aeron.connect();

    ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;

    AeronServerTransport aeronServerTransport =
        new AeronServerTransport(
            aeron,
            aeronUrl,
            Schedulers.newSingle("AeronBossScheduler"),
            new EventLoopGroup(2, () -> new BackoffIdleStrategy(100, 1000, 10000, 1000000)),
            new BackoffIdleStrategy(100, 1000, 10000, 1000000),
            allocator,
            256,
            32,
            SECONDS.toNanos(1));

    //    TcpServerTransport tcpServerTransport = TcpServerTransport.create(8080);
    //
    //    WebsocketServerTransport websocketServerTransport = WebsocketServerTransport.create(8080);
    //
    final Closeable server =
        RSocketServer.create(new PingHandler())
            .payloadDecoder(PayloadDecoder.ZERO_COPY)
            .bindNow(aeronServerTransport);

    RuntimeMXBean bean = ManagementFactory.getRuntimeMXBean();

    // Get name representing the running Java virtual machine.
    // It returns something like 6460@AURORA. Where the value
    // before the @ symbol is the PID.
    String jvmName = bean.getName();
    System.out.println("Name = " + jvmName);

    // Extract the PID by splitting the string returned by the
    // bean.getName() method.
    long pid = Long.valueOf(jvmName.split("@")[0]);

    System.out.println("Server Started on channel[" + aeronUrl + "]. PID  = " + pid);

    server.onClose().block();
  }

  public static class PingHandler implements SocketAcceptor {
    private final Payload pong;

    public PingHandler() {
      byte[] data = new byte[1024];
      ThreadLocalRandom.current().nextBytes(data);
      this.pong = ByteBufPayload.create(data);
    }

    public PingHandler(byte[] data) {
      this.pong = ByteBufPayload.create(data);
    }

    public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
      return Mono.just(
          new RSocket() {
            public Mono<Payload> requestResponse(Payload payload) {
              payload.release();
              return Mono.just(PingHandler.this.pong.retain());
            }

            public Flux<Payload> requestStream(Payload payload) {
              payload.release();
              return Flux.range(0, 100).map((v) -> PingHandler.this.pong.retain()).log();
            }
          });
    }
  }
}
