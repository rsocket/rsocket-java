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

package io.rsocket.transport.netty.server;

import static io.rsocket.frame.FrameLengthFlyweight.FRAME_LENGTH_MASK;

import io.netty.buffer.ByteBufAllocator;
import io.rsocket.DuplexConnection;
import io.rsocket.fragmentation.FragmentationDuplexConnection;
import io.rsocket.fragmentation.ReassemblyDuplexConnection;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.TransportHeaderAware;
import io.rsocket.transport.netty.WebsocketDuplexConnection;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.http.server.HttpServer;
import reactor.netty.http.server.WebsocketServerSpec;

/**
 * An implementation of {@link ServerTransport} that connects to a {@link ClientTransport} via a
 * Websocket.
 */
public final class WebsocketServerTransport extends BaseWebsocketServerTransport<CloseableChannel>
    implements TransportHeaderAware {
  private static final Logger logger = LoggerFactory.getLogger(WebsocketServerTransport.class);

  private final HttpServer server;

  private Supplier<Map<String, String>> transportHeaders = Collections::emptyMap;

  private WebsocketServerTransport(HttpServer server) {
    this.server = serverConfigurer.apply(Objects.requireNonNull(server, "server must not be null"));
  }

  /**
   * Creates a new instance binding to localhost
   *
   * @param port the port to bind to
   * @return a new instance
   */
  public static WebsocketServerTransport create(int port) {
    HttpServer httpServer = HttpServer.create().port(port);
    return create(httpServer);
  }

  /**
   * Creates a new instance
   *
   * @param bindAddress the address to bind to
   * @param port the port to bind to
   * @return a new instance
   * @throws NullPointerException if {@code bindAddress} is {@code null}
   */
  public static WebsocketServerTransport create(String bindAddress, int port) {
    Objects.requireNonNull(bindAddress, "bindAddress must not be null");

    HttpServer httpServer = HttpServer.create().host(bindAddress).port(port);
    return create(httpServer);
  }

  /**
   * Creates a new instance
   *
   * @param address the address to bind to
   * @return a new instance
   * @throws NullPointerException if {@code address} is {@code null}
   */
  public static WebsocketServerTransport create(InetSocketAddress address) {
    Objects.requireNonNull(address, "address must not be null");

    return create(address.getHostName(), address.getPort());
  }

  /**
   * Creates a new instance
   *
   * @param server the {@link HttpServer} to use
   * @return a new instance
   * @throws NullPointerException if {@code server} is {@code null}
   */
  public static WebsocketServerTransport create(final HttpServer server) {
    Objects.requireNonNull(server, "server must not be null");

    return new WebsocketServerTransport(server);
  }

  @Override
  public void setTransportHeaders(Supplier<Map<String, String>> transportHeaders) {
    this.transportHeaders =
        Objects.requireNonNull(transportHeaders, "transportHeaders must not be null");
  }

  @Override
  public Mono<CloseableChannel> start(ConnectionAcceptor acceptor, int mtu) {
    Objects.requireNonNull(acceptor, "acceptor must not be null");

    Mono<CloseableChannel> isError = FragmentationDuplexConnection.checkMtu(mtu);
    return isError != null
        ? isError
        : server
            .handle(
                (request, response) -> {
                  transportHeaders.get().forEach(response::addHeader);
                  return response.sendWebsocket(
                      (in, out) -> {
                        DuplexConnection connection =
                            new ReassemblyDuplexConnection(
                                new WebsocketDuplexConnection((Connection) in),
                                ByteBufAllocator.DEFAULT,
                                false);
                        if (mtu > 0) {
                          connection =
                              new FragmentationDuplexConnection(
                                  connection, ByteBufAllocator.DEFAULT, mtu, false, "server");
                        }
                        return acceptor.apply(connection).then(out.neverComplete());
                      },
                      WebsocketServerSpec.builder()
                          .maxFramePayloadLength(FRAME_LENGTH_MASK)
                          .build());
                })
            .bind()
            .map(CloseableChannel::new);
  }
}
