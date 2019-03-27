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
import io.rsocket.DuplexConnection;
import io.rsocket.Frame;
import io.rsocket.internal.BaseDuplexConnection;
import java.util.Objects;
import org.reactivestreams.Publisher;
import reactor.core.Fuseable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;

/** An implementation of {@link DuplexConnection} that connects via TCP. */
public final class TcpDuplexConnection extends BaseDuplexConnection {

  private final Connection connection;

  /**
   * Creates a new instance
   *
   * @param connection the {@link Connection} to for managing the server
   */
  public TcpDuplexConnection(Connection connection) {
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
  public Flux<Frame> receive() {
    return connection.inbound().receive().map(buf -> Frame.from(buf.retain()));
  }

  @Override
  public Mono<Void> send(Publisher<Frame> frames) {
    return Flux.from(frames)
        .transform(
            frameFlux -> {
              if (frameFlux instanceof Fuseable.QueueSubscription) {
                Fuseable.QueueSubscription<Frame> queueSubscription =
                    (Fuseable.QueueSubscription<Frame>) frameFlux;
                queueSubscription.requestFusion(Fuseable.ASYNC);
                return new SendPublisher<>(
                    queueSubscription,
                    frameFlux,
                    connection.channel(),
                    frame -> frame.content().retain(),
                    ByteBuf::readableBytes);
              } else {
                return new SendPublisher<>(
                    frameFlux,
                    connection.channel(),
                    frame -> frame.content().retain(),
                    ByteBuf::readableBytes);
              }
            })
        .then();
  }
}
