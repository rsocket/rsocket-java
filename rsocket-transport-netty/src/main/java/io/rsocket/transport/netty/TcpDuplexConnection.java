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
import io.rsocket.DuplexConnection;
import io.rsocket.RSocketErrorException;
import io.rsocket.frame.ErrorFrameCodec;
import io.rsocket.frame.FrameLengthCodec;
import io.rsocket.internal.BaseDuplexConnection;
import java.net.SocketAddress;
import java.util.Objects;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;

/** An implementation of {@link DuplexConnection} that connects via TCP. */
public final class TcpDuplexConnection extends BaseDuplexConnection {
  private final String side;
  private final Connection connection;

  /**
   * Creates a new instance
   *
   * @param connection the {@link Connection} for managing the server
   */
  public TcpDuplexConnection(Connection connection) {
    this("unknown", connection);
  }

  /**
   * Creates a new instance
   *
   * @param connection the {@link Connection} for managing the server
   */
  public TcpDuplexConnection(String side, Connection connection) {
    this.connection = Objects.requireNonNull(connection, "connection must not be null");
    this.side = side;

    connection.outbound().send(sender).then().doFinally(__ -> connection.dispose()).subscribe();
  }

  @Override
  public ByteBufAllocator alloc() {
    return connection.channel().alloc();
  }

  @Override
  public SocketAddress remoteAddress() {
    return connection.channel().remoteAddress();
  }

  @Override
  protected void doOnClose() {
    connection.dispose();
  }

  @Override
  public Mono<Void> onClose() {
    return Mono.whenDelayError(super.onClose(), connection.onTerminate());
  }

  @Override
  public void sendErrorAndClose(RSocketErrorException e) {
    final ByteBuf errorFrame = ErrorFrameCodec.encode(alloc(), 0, e);
    sender.tryEmitFinal(FrameLengthCodec.encode(alloc(), errorFrame.readableBytes(), errorFrame));
  }

  @Override
  public Flux<ByteBuf> receive() {
    return connection.inbound().receive().map(FrameLengthCodec::frame);
  }

  @Override
  public void sendFrame(int streamId, ByteBuf frame) {
    super.sendFrame(streamId, FrameLengthCodec.encode(alloc(), frame.readableBytes(), frame));
  }

  @Override
  public String toString() {
    return "TcpDuplexConnection{" + "side='" + side + '\'' + ", connection=" + connection + '}';
  }
}
