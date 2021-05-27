/*
 * Copyright 2015-2021 the original author or authors.
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
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.rsocket.DuplexConnection;
import io.rsocket.RSocketErrorException;
import io.rsocket.frame.ErrorFrameCodec;
import io.rsocket.internal.BaseDuplexConnection;
import java.net.SocketAddress;
import java.util.Objects;
import reactor.core.publisher.Flux;
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

    connection.outbound().sendObject(sender.map(BinaryWebSocketFrame::new)).then().subscribe();
  }

  @Override
  public ByteBufAllocator alloc() {
    return connection.channel().alloc();
  }

  @Override
  public SocketAddress localAddress() {
    return connection.channel().localAddress();
  }

  @Override
  public SocketAddress remoteAddress() {
    return connection.channel().remoteAddress();
  }

  @Override
  protected void doOnClose() {
    sender.dispose();
    connection.dispose();
  }

  @Override
  public Flux<ByteBuf> receive() {
    return connection.inbound().receive();
  }

  @Override
  public void sendErrorAndClose(RSocketErrorException e) {
    final ByteBuf errorFrame = ErrorFrameCodec.encode(alloc(), 0, e);
    connection
        .outbound()
        .sendObject(new BinaryWebSocketFrame(errorFrame))
        .then()
        .subscribe(
            null,
            t -> onClose.tryEmitError(t),
            () -> {
              final Throwable cause = e.getCause();
              if (cause == null) {
                onClose.tryEmitEmpty();
              } else {
                onClose.tryEmitError(cause);
              }
            });
  }
}
