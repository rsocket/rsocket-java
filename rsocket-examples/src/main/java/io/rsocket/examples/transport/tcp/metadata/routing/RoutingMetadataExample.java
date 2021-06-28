/*
 * Copyright 2015-Present the original author or authors.
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

package io.rsocket.examples.transport.tcp.metadata.routing;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.metadata.RoutingMetadata;
import io.rsocket.metadata.TaggingMetadataCodec;
import io.rsocket.metadata.WellKnownMimeType;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.ByteBufPayload;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class RoutingMetadataExample {
  static final Logger logger = LoggerFactory.getLogger(RoutingMetadataExample.class);

  public static void main(String[] args) {
    RSocketServer.create(
            SocketAcceptor.forRequestResponse(
                payload -> {
                  final String route = decodeRoute(payload.sliceMetadata());

                  logger.info("Received RequestResponse[route={}]", route);

                  if ("my.test.route".equals(route)) {
                    payload.release();
                    return Mono.just(ByteBufPayload.create("Hello From My Test Route"));
                  }

                  return Mono.error(new IllegalArgumentException("Route " + route + " not found"));
                }))
        .bindNow(TcpServerTransport.create("localhost", 7000));

    RSocket socket =
        RSocketConnector.create()
            // here we specify that route will be encoded using
            // Routing&Tagging Metadata layout specified at this
            // subspec https://github.com/rsocket/rsocket/blob/master/Extensions/Routing.md
            .metadataMimeType(WellKnownMimeType.MESSAGE_RSOCKET_ROUTING.getString())
            .connect(TcpClientTransport.create("localhost", 7000))
            .block();

    final ByteBuf routeMetadata =
        TaggingMetadataCodec.createTaggingContent(
            ByteBufAllocator.DEFAULT, Collections.singletonList("my.test.route"));
    socket
        .requestResponse(
            ByteBufPayload.create(
                ByteBufUtil.writeUtf8(ByteBufAllocator.DEFAULT, "HelloWorld"), routeMetadata))
        .log()
        .block();
  }

  static String decodeRoute(ByteBuf metadata) {
    final RoutingMetadata routingMetadata = new RoutingMetadata(metadata);

    return routingMetadata.iterator().next();
  }
}
