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

package io.rsocket.examples.transport.ws;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.rsocket.DuplexConnection;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.fragmentation.ReassemblyDuplexConnection;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.netty.WebsocketDuplexConnection;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import io.rsocket.util.ByteBufPayload;
import java.time.Duration;
import java.util.HashMap;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.netty.Connection;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;

public class WebSocketHeadersSample {
  static final Payload payload1 = ByteBufPayload.create("Hello ");

  public static void main(String[] args) {

    ServerTransport.ConnectionAcceptor acceptor =
        RSocketServer.create(SocketAcceptor.with(new ServerRSocket()))
            .payloadDecoder(PayloadDecoder.ZERO_COPY)
            .asConnectionAcceptor();

    DisposableServer disposableServer =
        HttpServer.create()
            .host("localhost")
            .port(0)
            .route(
                routes ->
                    routes.ws(
                        "/",
                        (in, out) -> {
                          if (in.headers().containsValue("Authorization", "test", true)) {
                            DuplexConnection connection =
                                new ReassemblyDuplexConnection(
                                    new WebsocketDuplexConnection((Connection) in), false);
                            return acceptor.apply(connection).then(out.neverComplete());
                          }

                          return out.sendClose(
                              HttpResponseStatus.UNAUTHORIZED.code(),
                              HttpResponseStatus.UNAUTHORIZED.reasonPhrase());
                        }))
            .bindNow();

    WebsocketClientTransport clientTransport =
        WebsocketClientTransport.create(disposableServer.host(), disposableServer.port());

    clientTransport.setTransportHeaders(
        () -> {
          HashMap<String, String> map = new HashMap<>();
          map.put("Authorization", "test");
          return map;
        });

    RSocket socket =
        RSocketConnector.create()
            .keepAlive(Duration.ofMinutes(10), Duration.ofMinutes(10))
            .payloadDecoder(PayloadDecoder.ZERO_COPY)
            .connect(clientTransport)
            .block();

    Flux.range(0, 100)
        .concatMap(i -> socket.fireAndForget(payload1.retain()))
        //        .doOnNext(p -> {
        ////            System.out.println(p.getDataUtf8());
        //            p.release();
        //        })
        .blockLast();
    socket.dispose();

    WebsocketClientTransport clientTransport2 =
        WebsocketClientTransport.create(disposableServer.host(), disposableServer.port());

    RSocket rSocket =
        RSocketConnector.create()
            .keepAlive(Duration.ofMinutes(10), Duration.ofMinutes(10))
            .payloadDecoder(PayloadDecoder.ZERO_COPY)
            .connect(clientTransport2)
            .block();

    // expect error here because of closed channel
    rSocket.requestResponse(payload1).block();
  }

  private static class ServerRSocket implements RSocket {

    @Override
    public Mono<Void> fireAndForget(Payload payload) {
      // System.out.println(payload.getDataUtf8());
      payload.release();
      return Mono.empty();
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
      return Mono.just(payload);
    }

    @Override
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
      return Flux.from(payloads).subscribeOn(Schedulers.single());
    }
  }
}
