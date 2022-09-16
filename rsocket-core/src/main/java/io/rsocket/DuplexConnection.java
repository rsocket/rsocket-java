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

package io.rsocket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;

/** Represents a connection with input/output that the protocol uses. */
public interface DuplexConnection extends Availability, Closeable {

  /**
   * Delivers the given frame to the underlying transport connection. This method is non-blocking
   * and can be safely executed from multiple threads. This method does not provide any flow-control
   * mechanism.
   *
   * @param streamId to which the given frame relates
   * @param frame with the encoded content
   */
  void sendFrame(int streamId, ByteBuf frame);

  /**
   * Send an error frame and after it is successfully sent, close the connection.
   *
   * @param errorException to encode in the error frame
   */
  void sendErrorAndClose(RSocketErrorException errorException);

  /**
   * Returns a stream of all {@code Frame}s received on this connection.
   *
   * <p><strong>Completion</strong>
   *
   * <p>Returned {@code Publisher} <em>MUST</em> never emit a completion event ({@link
   * Subscriber#onComplete()}).
   *
   * <p><strong>Error</strong>
   *
   * <p>Returned {@code Publisher} can error with various transport errors. If the underlying
   * physical connection is closed by the peer, then the returned stream from here <em>MUST</em>
   * emit an {@link ClosedChannelException}.
   *
   * <p><strong>Multiple Subscriptions</strong>
   *
   * <p>Returned {@code Publisher} is not required to support multiple concurrent subscriptions.
   * RSocket will never have multiple subscriptions to this source. Implementations <em>MUST</em>
   * emit an {@link IllegalStateException} for subsequent concurrent subscriptions, if they do not
   * support multiple concurrent subscriptions.
   *
   * @return Stream of all {@code Frame}s received.
   */
  Flux<ByteBuf> receive();

  /**
   * Returns the assigned {@link ByteBufAllocator}.
   *
   * @return the {@link ByteBufAllocator}
   */
  ByteBufAllocator alloc();

  /**
   * Return the local address that this connection is connected to. The returned {@link
   * SocketAddress} varies by transport type and should be downcast to obtain more detailed
   * information. For TCP and WebSocket, the address type is {@link java.net.InetSocketAddress}. For
   * local transport, it is {@link io.rsocket.transport.local.LocalSocketAddress}.
   *
   * @return the address
   * @since 1.1.1
   */
  SocketAddress localAddress();

  /**
   * Return the remote address that this connection is connected to. The returned {@link
   * SocketAddress} varies by transport type and should be downcast to obtain more detailed
   * information. For TCP and WebSocket, the address type is {@link java.net.InetSocketAddress}. For
   * local transport, it is {@link io.rsocket.transport.local.LocalSocketAddress}.
   *
   * @return the address
   * @since 1.1
   */
  SocketAddress remoteAddress();

  @Override
  default double availability() {
    return isDisposed() ? 0.0 : 1.0;
  }
}
