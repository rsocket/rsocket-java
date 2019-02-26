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
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.DuplexConnection;
import io.rsocket.frame.FrameLengthFlyweight;
import java.util.Objects;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.Fuseable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.FutureMono;

/** An implementation of {@link DuplexConnection} that connects via TCP. */
public final class TcpDuplexConnection implements DuplexConnection {

  private final Connection connection;
  private final Disposable channelClosed;
  private final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
  private final boolean encodeLength;

  /**
   * Creates a new instance
   *
   * @param connection the {@link Connection} to for managing the server
   */
  public TcpDuplexConnection(Connection connection) {
    this(connection, true);
  }

  /**
   * Creates a new instance
   *
   * @param encodeLength indicates if this connection should encode the length or not.
   * @param connection the {@link Connection} to for managing the server
   */
  public TcpDuplexConnection(Connection connection, boolean encodeLength) {
    this.encodeLength = encodeLength;
    this.connection = Objects.requireNonNull(connection, "connection must not be null");
    this.channelClosed =
        FutureMono.from(connection.channel().closeFuture())
            .doFinally(
                s -> {
                  if (!isDisposed()) {
                    dispose();
                  }
                })
            .subscribe();
  }

  @Override
  public void dispose() {
    connection.dispose();
  }

  @Override
  public boolean isDisposed() {
    return connection.isDisposed();
  }

  @Override
  public Mono<Void> onClose() {
    return connection
        .onDispose()
        .doFinally(
            s -> {
              if (!channelClosed.isDisposed()) {
                channelClosed.dispose();
              }
            });
  }

  @Override
  public Flux<ByteBuf> receive() {
    return connection.inbound().receive().map(this::decode);
  }

  @Override
  public Mono<Void> send(Publisher<ByteBuf> frames) {
    return Flux.from(frames)
        .transform(
            frameFlux -> {
              if (frameFlux instanceof Fuseable.QueueSubscription) {
                Fuseable.QueueSubscription<ByteBuf> queueSubscription =
                    (Fuseable.QueueSubscription<ByteBuf>) frameFlux;
                queueSubscription.requestFusion(Fuseable.ASYNC);
                return new SendPublisher<>(
                    queueSubscription,
                    frameFlux,
                    connection.channel(),
                    this::encode,
                    ByteBuf::readableBytes);
              } else {
                return new SendPublisher<>(
                    frameFlux, connection.channel(), this::encode, ByteBuf::readableBytes);
              }
            })
        .then();
  }

  private ByteBuf encode(ByteBuf frame) {
    if (encodeLength) {
      return FrameLengthFlyweight.encode(allocator, frame.readableBytes(), frame).retain();
    } else {
      return frame;
    }
  }

  private ByteBuf decode(ByteBuf frame) {
    if (encodeLength) {
      return FrameLengthFlyweight.frame(frame).retain();
    } else {
      return frame;
    }
  }
}
