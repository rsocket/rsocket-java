/*
 * Copyright 2015-2018 the original author or authors.
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

package io.rsocket.examples.transport.ws;

import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus;
import io.rsocket.AbstractRSocket;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.SocketAcceptor;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.WebsocketRouteTransport;
import io.rsocket.util.ByteBufPayload;
import java.time.Duration;
import java.util.HashMap;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.scheduler.Schedulers;
import reactor.netty.http.server.HttpServer;

public class WebSocketRouteTransportHeadersSample {
  static final Payload payload1 = ByteBufPayload.create("Hello ");

  public static void main(String[] args) {

    CloseableChannel disposableServer =
        RSocketFactory.receive()
            .frameDecoder(PayloadDecoder.ZERO_COPY)
            .acceptor(new SocketAcceptorImpl())
            .transport(
                // Same could be done with routing transport
                WebsocketRouteTransport.builder()
                    .filteringInbound(
                        headers -> headers.containsValue("Authorization", "test", true))
                    .closingWithStatus(headers -> new WebSocketCloseStatus(4404, "Unauthorized"))
                    .observingOn("/")
                    .build(HttpServer.create().host("localhost").port(8080)))
            .start()
            .block();

    WebsocketClientTransport clientTransport =
        WebsocketClientTransport.create(disposableServer.address());

    MonoProcessor<WebSocketCloseStatus> statusMonoProcessor = MonoProcessor.create();
    clientTransport.setTransportHeaders(
        () -> {
          HashMap<String, String> map = new HashMap<>();
          map.put("Authorization", "1");
          return map;
        });

    clientTransport.setCloseStatusConsumer(
        webSocketCloseStatusMono -> webSocketCloseStatusMono.log().subscribe(statusMonoProcessor));

    RSocket socket =
        RSocketFactory.connect()
            .keepAliveAckTimeout(Duration.ofMinutes(10))
            .frameDecoder(PayloadDecoder.ZERO_COPY)
            .transport(clientTransport)
            .start()
            .block();

    try {
      Flux.range(0, 100).concatMap(i -> socket.fireAndForget(payload1.retain())).blockLast();

    } catch (Exception e) {
      System.out.println("Observed WebSocket Close Status " + statusMonoProcessor.peek());
    }

    socket.dispose();

    WebsocketClientTransport clientTransport2 =
        WebsocketClientTransport.create(disposableServer.address());

    clientTransport2.setTransportHeaders(
        () -> {
          HashMap<String, String> map = new HashMap<>();
          map.put("Authorization", "test");
          return map;
        });

    RSocket rSocket =
        RSocketFactory.connect()
            .keepAliveAckTimeout(Duration.ofMinutes(10))
            .frameDecoder(PayloadDecoder.ZERO_COPY)
            .transport(clientTransport2)
            .start()
            .block();

    // expect normal execution here
    rSocket.requestResponse(payload1).block();
  }

  private static class SocketAcceptorImpl implements SocketAcceptor {
    @Override
    public Mono<RSocket> accept(ConnectionSetupPayload setupPayload, RSocket reactiveSocket) {
      return Mono.just(
          new AbstractRSocket() {

            @Override
            public Mono<Void> fireAndForget(Payload payload) {
              payload.release();
              return Mono.empty();
            }

            @Override
            public Mono<Payload> requestResponse(Payload payload) {
              return Mono.just(payload);
            }

            @Override
            public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
              return Flux.from(payloads);
            }
          });
    }
  }
}
