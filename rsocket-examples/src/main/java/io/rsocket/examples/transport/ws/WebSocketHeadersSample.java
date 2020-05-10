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
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.frame.FrameLengthCodec;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.WebsocketServerTransport;
import io.rsocket.util.ByteBufPayload;
import java.time.Duration;
import java.util.function.BiFunction;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.WebsocketClientSpec;
import reactor.netty.http.server.HttpServer;
import reactor.netty.http.server.HttpServerRequest;
import reactor.netty.http.server.HttpServerResponse;
import reactor.netty.http.server.WebsocketServerSpec;
import reactor.netty.http.websocket.WebsocketInbound;
import reactor.netty.http.websocket.WebsocketOutbound;

public class WebSocketHeadersSample {
  static final Payload payload1 = ByteBufPayload.create("Hello ");

  public static void main(String[] args) {

    CloseableChannel closeableChannel =
        RSocketServer.create(SocketAcceptor.with(new ServerRSocket()))
            .payloadDecoder(PayloadDecoder.ZERO_COPY)
            .bindNow(
                WebsocketServerTransport.create(
                    HttpServer.create().host("localhost").port(0),
                    new WebsocketServerTransport.Handler() {
                      @Override
                      public Publisher<Void> handle(
                          HttpServerRequest request,
                          HttpServerResponse response,
                          BiFunction<WebsocketInbound, WebsocketOutbound, Publisher<Void>>
                              webSocketHandler) {

                        if (request.requestHeaders().containsValue("Authorization", "test", true)) {
                          return response.sendWebsocket(
                              webSocketHandler,
                              WebsocketServerSpec.builder()
                                  .maxFramePayloadLength(FrameLengthCodec.FRAME_LENGTH_MASK)
                                  .build());
                        }

                        return response.status(HttpResponseStatus.UNAUTHORIZED.code()).send();
                      }
                    }));

    WebsocketClientTransport clientTransport =
        WebsocketClientTransport.create(
            () ->
                HttpClient.create()
                    .remoteAddress(closeableChannel::address)
                    .headers(headers -> headers.add("Authorization", "test"))
                    .websocket(
                        WebsocketClientSpec.builder()
                            .maxFramePayloadLength(FrameLengthCodec.FRAME_LENGTH_MASK)
                            .build())
                    .connect());

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
        WebsocketClientTransport.create(
            () ->
                HttpClient.create()
                    .remoteAddress(closeableChannel::address)
                    .websocket(
                        WebsocketClientSpec.builder()
                            .maxFramePayloadLength(FrameLengthCodec.FRAME_LENGTH_MASK)
                            .build())
                    .connect());

    // expect error here because of closed channel
    RSocketConnector.create()
        .keepAlive(Duration.ofMinutes(10), Duration.ofMinutes(10))
        .payloadDecoder(PayloadDecoder.ZERO_COPY)
        .connect(clientTransport2)
        .block();

    System.out.println("Should never happen");
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
