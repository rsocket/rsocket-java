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

package io.rsocket.transport.netty.client;

import io.rsocket.DuplexConnection;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.netty.RSocketLengthCodec;
import io.rsocket.transport.netty.TcpDuplexConnection;
import java.net.InetSocketAddress;
import java.util.Objects;
import reactor.core.publisher.Mono;
import reactor.netty.tcp.TcpClient;

/**
 * An implementation of {@link ClientTransport} that connects to a {@link ServerTransport} via TCP.
 */
public final class TcpClientTransport implements ClientTransport {

  private final TcpClient client;

  private TcpClientTransport(TcpClient client) {
    this.client = client;
  }

  /**
   * Creates a new instance connecting to localhost
   *
   * @param port the port to connect to
   * @return a new instance
   */
  public static TcpClientTransport create(int port) {
    TcpClient tcpClient = TcpClient.create().port(port);
    return create(tcpClient);
  }

  /**
   * Creates a new instance
   *
   * @param bindAddress the address to connect to
   * @param port the port to connect to
   * @return a new instance
   * @throws NullPointerException if {@code bindAddress} is {@code null}
   */
  public static TcpClientTransport create(String bindAddress, int port) {
    Objects.requireNonNull(bindAddress, "bindAddress must not be null");

    TcpClient tcpClient = TcpClient.create().host(bindAddress).port(port);
    return create(tcpClient);
  }

  /**
   * Creates a new instance
   *
   * @param address the address to connect to
   * @return a new instance
   * @throws NullPointerException if {@code address} is {@code null}
   */
  public static TcpClientTransport create(InetSocketAddress address) {
    Objects.requireNonNull(address, "address must not be null");

    TcpClient tcpClient = TcpClient.create().addressSupplier(() -> address);
    return create(tcpClient);
  }

  /**
   * Creates a new instance
   *
   * @param client the {@link TcpClient} to use
   * @return a new instance
   * @throws NullPointerException if {@code client} is {@code null}
   */
  public static TcpClientTransport create(TcpClient client) {
    Objects.requireNonNull(client, "client must not be null");

    return new TcpClientTransport(client);
  }

  @Override
  public Mono<DuplexConnection> connect() {
    return client
      .doOnConnected(c -> c.addHandlerLast(new RSocketLengthCodec()))
      .connect()
      .map(TcpDuplexConnection::new);
  }
}
