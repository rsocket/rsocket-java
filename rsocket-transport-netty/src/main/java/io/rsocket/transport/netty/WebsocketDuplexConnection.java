/*
 * Copyright 2015-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.rsocket.transport.netty;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.rsocket.DuplexConnection;
import io.rsocket.internal.BaseDuplexConnection;
import java.util.Objects;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;

/**
 * An implementation of {@link DuplexConnection} that connects via a Websocket.
 *
 * <p>rsocket-java strongly assumes that each ByteBuf is encoded with the length. This is not true
 * for message oriented transports so this must be specifically dropped from Frames sent and
 * stitched back on for frames received.
 */
public final class WebsocketDuplexConnection extends BaseDuplexConnection {

  private final Connection connection;

  /**
   * Creates a new instance
   *
   * @param connection the {@link Connection} to for managing the server
   */
  public WebsocketDuplexConnection(Connection connection) {
    this.connection = Objects.requireNonNull(connection, "connection must not be null");

    connection
        .channel()
        .closeFuture()
        .addListener(
            future -> {
              if (!isDisposed()) dispose();
            });
  }

  @Override
  protected void doOnClose() {
    if (!connection.isDisposed()) {
      connection.dispose();
    }
  }

  @Override
  public Flux<ByteBuf> receive() {
    return connection.inbound().receive().map(ByteBuf::retain);
  }

  @Override
  public Mono<Void> send(Publisher<ByteBuf> frames) {
    if (frames instanceof Mono) {
      return connection
          .outbound()
          .sendObject(((Mono<ByteBuf>) frames).map(BinaryWebSocketFrame::new))
          .then();
    }
    return connection
        .outbound()
        .sendObject(Flux.from(frames).map(BinaryWebSocketFrame::new))
        .then();
  }
}
