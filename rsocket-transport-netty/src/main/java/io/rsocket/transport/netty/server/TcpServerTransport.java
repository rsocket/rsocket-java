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

package io.rsocket.transport.netty.server;

import static io.rsocket.frame.FrameLengthCodec.FRAME_LENGTH_MASK;

import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.netty.RSocketLengthCodec;
import io.rsocket.transport.netty.TcpDuplexConnection;
import java.net.InetSocketAddress;
import java.util.Objects;
import reactor.core.publisher.Mono;
import reactor.netty.tcp.TcpServer;

/**
 * An implementation of {@link ServerTransport} that connects to a {@link ClientTransport} via TCP.
 */
public final class TcpServerTransport implements ServerTransport<CloseableChannel> {

  private final TcpServer server;
  private final int maxFrameLength;

  private TcpServerTransport(TcpServer server, int maxFrameLength) {
    this.server = server;
    this.maxFrameLength = maxFrameLength;
  }

  /**
   * Creates a new instance binding to localhost
   *
   * @param port the port to bind to
   * @return a new instance
   */
  public static TcpServerTransport create(int port) {
    TcpServer server = TcpServer.create().port(port);
    return create(server);
  }

  /**
   * Creates a new instance
   *
   * @param bindAddress the address to bind to
   * @param port the port to bind to
   * @return a new instance
   * @throws NullPointerException if {@code bindAddress} is {@code null}
   */
  public static TcpServerTransport create(String bindAddress, int port) {
    Objects.requireNonNull(bindAddress, "bindAddress must not be null");
    TcpServer server = TcpServer.create().host(bindAddress).port(port);
    return create(server);
  }

  /**
   * Creates a new instance
   *
   * @param address the address to bind to
   * @return a new instance
   * @throws NullPointerException if {@code address} is {@code null}
   */
  public static TcpServerTransport create(InetSocketAddress address) {
    Objects.requireNonNull(address, "address must not be null");
    return create(address.getHostName(), address.getPort());
  }

  /**
   * Creates a new instance
   *
   * @param server the {@link TcpServer} to use
   * @return a new instance
   * @throws NullPointerException if {@code server} is {@code null}
   */
  public static TcpServerTransport create(TcpServer server) {
    return create(server, FRAME_LENGTH_MASK);
  }

  /**
   * Creates a new instance
   *
   * @param server the {@link TcpServer} to use
   * @param maxFrameLength max frame length being sent over the connection
   * @return a new instance
   * @throws NullPointerException if {@code server} is {@code null}
   */
  public static TcpServerTransport create(TcpServer server, int maxFrameLength) {
    Objects.requireNonNull(server, "server must not be null");
    return new TcpServerTransport(server, maxFrameLength);
  }

  @Override
  public int maxFrameLength() {
    return maxFrameLength;
  }

  @Override
  public Mono<CloseableChannel> start(ConnectionAcceptor acceptor) {
    Objects.requireNonNull(acceptor, "acceptor must not be null");
    return server
        .doOnConnection(
            c -> {
              c.addHandlerLast(new RSocketLengthCodec(maxFrameLength));
              acceptor
                  .apply(new TcpDuplexConnection("server", c))
                  .then(Mono.<Void>never())
                  .subscribe(c.disposeSubscriber());
            })
        .bind()
        .map(CloseableChannel::new);
  }
}
