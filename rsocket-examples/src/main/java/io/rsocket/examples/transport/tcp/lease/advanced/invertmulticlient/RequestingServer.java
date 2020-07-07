/*
 * Copyright 2015-2020 the original author or authors.
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

package io.rsocket.examples.transport.tcp.lease.advanced.invertmulticlient;

import io.rsocket.RSocket;
import io.rsocket.core.RSocketServer;
import io.rsocket.examples.transport.tcp.lease.advanced.common.DefaultDeferringLeaseReceiver;
import io.rsocket.examples.transport.tcp.lease.advanced.common.DeferringLeaseReceiver;
import io.rsocket.examples.transport.tcp.lease.advanced.common.LeaseWaitingRSocket;
import io.rsocket.lease.Leases;
import io.rsocket.plugins.RSocketInterceptor;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.ByteBufPayload;
import java.util.Comparator;
import java.util.UUID;
import java.util.concurrent.PriorityBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RequestingServer {

  private static final Logger logger = LoggerFactory.getLogger(RequestingServer.class);

  static final ThreadLocal<DeferringLeaseReceiver> CURRENT_LEASE_RECEIVER = new ThreadLocal<>();

  public static void main(String[] args) {
    PriorityBlockingQueue<RSocket> rSockets =
        new PriorityBlockingQueue<>(
            16, Comparator.comparingDouble(RSocket::availability).reversed());

    CloseableChannel server =
        RSocketServer.create(
                (setup, sendingSocket) -> {
                  logger.info("Received new connection");
                  return Mono.<RSocket>just(new RSocket() {})
                      .doAfterTerminate(() -> rSockets.put(sendingSocket));
                })
            .lease(
                () -> {
                  DefaultDeferringLeaseReceiver leaseReceiver =
                      new DefaultDeferringLeaseReceiver(UUID.randomUUID().toString());
                  CURRENT_LEASE_RECEIVER.set(leaseReceiver);
                  return Leases.create().receiver(leaseReceiver);
                })
            .interceptors(
                ir ->
                    ir.forRequester(
                        (RSocketInterceptor)
                            r -> new LeaseWaitingRSocket(r, CURRENT_LEASE_RECEIVER.get())))
            .bindNow(TcpServerTransport.create("localhost", 7000));

    logger.info("Server started on port {}", server.address().getPort());

    // generate stream of fnfs
    Flux.generate(
            () -> 0L,
            (state, sink) -> {
              sink.next(state);
              return state + 1;
            })
        .flatMap(
            tick -> {
              logger.info("Requesting FireAndForget({})", tick);

              return Mono.fromCallable(
                      () -> {
                        RSocket rSocket = rSockets.take();
                        rSockets.offer(rSocket);
                        return rSocket;
                      })
                  .flatMap(
                      clientRSocket ->
                          clientRSocket.fireAndForget(ByteBufPayload.create("" + tick)))
                  .retry();
            })
        .blockLast();

    server.onClose().block();
  }
}
