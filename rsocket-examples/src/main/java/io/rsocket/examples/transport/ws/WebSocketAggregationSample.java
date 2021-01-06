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

package io.rsocket.examples.transport.ws;

import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.netty.WebsocketDuplexConnection;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import io.rsocket.util.ByteBufPayload;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;

public class WebSocketAggregationSample {

  private static final Logger logger = LoggerFactory.getLogger(WebSocketAggregationSample.class);

  public static void main(String[] args) {

    ServerTransport.ConnectionAcceptor connectionAcceptor =
        RSocketServer.create(SocketAcceptor.forRequestResponse(Mono::just))
            .payloadDecoder(PayloadDecoder.ZERO_COPY)
            .asConnectionAcceptor();

    DisposableServer server =
        HttpServer.create()
            .host("localhost")
            .port(0)
            .handle(
                (req, res) ->
                    res.sendWebsocket(
                        (in, out) ->
                            connectionAcceptor
                                .apply(
                                    new WebsocketDuplexConnection(
                                        (Connection) in.aggregateFrames()))
                                .then(out.neverComplete())))
            .bindNow();

    WebsocketClientTransport transport =
        WebsocketClientTransport.create(server.host(), server.port());

    RSocket clientRSocket =
        RSocketConnector.create()
            .keepAlive(Duration.ofMinutes(10), Duration.ofMinutes(10))
            .payloadDecoder(PayloadDecoder.ZERO_COPY)
            .connect(transport)
            .block();

    Flux.range(1, 100)
        .concatMap(i -> clientRSocket.requestResponse(ByteBufPayload.create("Hello " + i)))
        .doOnNext(payload -> logger.debug("Processed " + payload.getDataUtf8()))
        .blockLast();
    clientRSocket.dispose();
    server.dispose();
  }
}
