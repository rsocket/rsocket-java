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

package io.rsocket.transport.netty;

import io.netty.channel.ChannelOption;
import io.rsocket.test.TransportTest;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.WebsocketServerTransport;
import java.net.InetSocketAddress;
import java.time.Duration;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.server.HttpServer;

final class WebsocketTransportTest extends TransportTest<InetSocketAddress, CloseableChannel> {

  @Override
  protected TransportPair<InetSocketAddress, CloseableChannel> createTransportPair() {
    return new TransportPair<>(
        () -> InetSocketAddress.createUnresolved("localhost", 0),
        (address, server, allocator) ->
            WebsocketClientTransport.create(
                HttpClient.create()
                    .host(server.address().getHostName())
                    .port(server.address().getPort())
                    .option(ChannelOption.ALLOCATOR, allocator),
                ""),
        (address, allocator) ->
            WebsocketServerTransport.create(
                HttpServer.create()
                    .host(address.getHostName())
                    .port(address.getPort())
                    .option(ChannelOption.ALLOCATOR, allocator)),
        Duration.ofMinutes(2));
  }
}
