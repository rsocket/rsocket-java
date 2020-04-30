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

package io.rsocket.examples.transport.tcp.lease;

import static java.time.Duration.ofSeconds;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.lease.Lease;
import io.rsocket.lease.LeaseStats;
import io.rsocket.lease.Leases;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import java.util.Date;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class LeaseExample {
  private static final String SERVER_TAG = "server";
  private static final String CLIENT_TAG = "client";

  public static void main(String[] args) {

    CloseableChannel server =
        RSocketServer.create(
                (setup, sendingRSocket) -> Mono.just(new ServerRSocket(sendingRSocket)))
            .lease(
                () ->
                    Leases.<NoopStats>create()
                        .sender(new LeaseSender(SERVER_TAG, 7_000, 5))
                        .receiver(new LeaseReceiver(SERVER_TAG))
                        .stats(new NoopStats()))
            .bind(TcpServerTransport.create("localhost", 7000))
            .block();

    RSocket clientRSocket =
        RSocketConnector.create()
            .lease(
                () ->
                    Leases.<NoopStats>create()
                        .sender(new LeaseSender(CLIENT_TAG, 3_000, 5))
                        .receiver(new LeaseReceiver(CLIENT_TAG)))
            .acceptor(
                SocketAcceptor.forRequestResponse(
                    payload -> Mono.just(DefaultPayload.create("Client Response " + new Date()))))
            .connect(TcpClientTransport.create(server.address()))
            .block();

    Flux.interval(ofSeconds(1))
        .flatMap(
            signal -> {
              System.out.println("Client requester availability: " + clientRSocket.availability());
              return clientRSocket
                  .requestResponse(DefaultPayload.create("Client request " + new Date()))
                  .doOnError(err -> System.out.println("Client request error: " + err))
                  .onErrorResume(err -> Mono.empty());
            })
        .subscribe(resp -> System.out.println("Client requester response: " + resp.getDataUtf8()));

    clientRSocket.onClose().block();
    server.dispose();
  }

  private static class LeaseSender implements Function<Optional<NoopStats>, Flux<Lease>> {
    private final String tag;
    private final int ttlMillis;
    private final int allowedRequests;

    public LeaseSender(String tag, int ttlMillis, int allowedRequests) {
      this.tag = tag;
      this.ttlMillis = ttlMillis;
      this.allowedRequests = allowedRequests;
    }

    @Override
    public Flux<Lease> apply(Optional<NoopStats> leaseStats) {
      System.out.println(
          String.format("%s stats are %s", tag, leaseStats.isPresent() ? "present" : "absent"));
      return Flux.interval(ofSeconds(1), ofSeconds(10))
          .onBackpressureLatest()
          .map(
              tick -> {
                System.out.println(
                    String.format(
                        "%s responder sends new leases: ttl: %d, requests: %d",
                        tag, ttlMillis, allowedRequests));
                return Lease.create(ttlMillis, allowedRequests);
              });
    }
  }

  private static class LeaseReceiver implements Consumer<Flux<Lease>> {
    private final String tag;

    public LeaseReceiver(String tag) {
      this.tag = tag;
    }

    @Override
    public void accept(Flux<Lease> receivedLeases) {
      receivedLeases.subscribe(
          l ->
              System.out.println(
                  String.format(
                      "%s received leases - ttl: %d, requests: %d",
                      tag, l.getTimeToLiveMillis(), l.getAllowedRequests())));
    }
  }

  private static class NoopStats implements LeaseStats {

    @Override
    public void onEvent(EventType eventType) {}
  }

  private static class ServerRSocket implements RSocket {
    private final RSocket senderRSocket;

    public ServerRSocket(RSocket senderRSocket) {
      this.senderRSocket = senderRSocket;
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
      System.out.println("Server requester availability: " + senderRSocket.availability());
      senderRSocket
          .requestResponse(DefaultPayload.create("Server request " + new Date()))
          .doOnError(err -> System.out.println("Server request error: " + err))
          .onErrorResume(err -> Mono.empty())
          .subscribe(
              resp -> System.out.println("Server requester response: " + resp.getDataUtf8()));

      return Mono.just(DefaultPayload.create("Server Response " + new Date()));
    }
  }
}
