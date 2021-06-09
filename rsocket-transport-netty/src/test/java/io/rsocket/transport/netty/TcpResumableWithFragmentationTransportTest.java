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
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import java.net.InetSocketAddress;
import java.time.Duration;
import reactor.netty.tcp.TcpClient;
import reactor.netty.tcp.TcpServer;

final class TcpResumableWithFragmentationTransportTest
    extends TransportTest<InetSocketAddress, CloseableChannel> {

  @Override
  protected TransportPair<InetSocketAddress, CloseableChannel> createTransportPair() {
    return new TransportPair<>(
        () -> InetSocketAddress.createUnresolved("localhost", 0),
        (address, server, allocator) ->
            TcpClientTransport.create(
                TcpClient.create()
                    .remoteAddress(server::address)
                    .option(ChannelOption.ALLOCATOR, allocator)),
        (address, allocator) ->
            TcpServerTransport.create(
                TcpServer.create()
                    .bindAddress(() -> address)
                    .option(ChannelOption.ALLOCATOR, allocator)),
        true,
        true,
        Duration.ofMinutes(2));
  }
}
