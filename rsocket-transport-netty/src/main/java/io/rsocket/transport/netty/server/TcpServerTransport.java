/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.transport.netty.server;

import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.netty.NettyDuplexConnection;
import io.rsocket.transport.netty.RSocketLengthCodec;
import java.net.InetSocketAddress;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.tcp.TcpServer;

public class TcpServerTransport implements ServerTransport<NettyContextCloseable> {
  TcpServer server;

  private TcpServerTransport(TcpServer server) {
    this.server = server;
  }

  public static TcpServerTransport create(InetSocketAddress address) {
    TcpServer server = TcpServer.create(address.getHostName(), address.getPort());
    return create(server);
  }

  public static TcpServerTransport create(String bindAddress, int port) {
    TcpServer server = TcpServer.create(bindAddress, port);
    return create(server);
  }

  public static TcpServerTransport create(int port) {
    TcpServer server = TcpServer.create(port);
    return create(server);
  }

  public static TcpServerTransport create(TcpServer server) {
    return new TcpServerTransport(server);
  }

  @Override
  public Mono<NettyContextCloseable> start(ConnectionAcceptor acceptor) {
    return server
        .newHandler(
            (in, out) -> {
              in.context().addHandler("server-length-codec", new RSocketLengthCodec());
              NettyDuplexConnection connection = new NettyDuplexConnection(in, out, in.context());
              acceptor.apply(connection).subscribe();

              return out.neverComplete();
            })
        .map(NettyContextCloseable::new);
  }
}
