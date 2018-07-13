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

import io.rsocket.Closeable;
import java.net.InetSocketAddress;
import java.util.Objects;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableChannel;

/**
 * An implementation of {@link Closeable} that wraps a {@link DisposableChannel}, enabling close-ability
 * and exposing the {@link DisposableChannel}'s address.
 */
public final class CloseableChannel implements Closeable {

  private DisposableChannel channel;

  /**
   * Creates a new instance
   *
   * @param channel the {@link DisposableChannel} to wrap
   * @throws NullPointerException if {@code context} is {@code null}
   */
  CloseableChannel(DisposableChannel channel) {
    this.channel = Objects.requireNonNull(channel, "channel must not be null");
  }

  /**
   * Return local server selector channel address.
   *
   * @return local {@link InetSocketAddress}
   * @see DisposableChannel#address()
   */
  public InetSocketAddress address() {
    return channel.address();
  }

  @Override
  public void dispose() {
    channel.dispose();
  }

  @Override
  public boolean isDisposed() {
    return channel.isDisposed();
  }

  @Override
  public Mono<Void> onClose() {
    return channel.onDispose();
  }
}
