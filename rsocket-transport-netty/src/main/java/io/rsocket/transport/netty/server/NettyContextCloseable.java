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
import reactor.ipc.netty.NettyContext;

/**
 * An implementation of {@link Closeable} that wraps a {@link NettyContext}, enabling close-ability
 * and exposing the {@link NettyContext}'s address.
 */
public final class NettyContextCloseable implements Closeable {

  private NettyContext context;

  /**
   * Creates a new instance
   *
   * @param context the {@link NettyContext} to wrap
   * @throws NullPointerException if {@code context} is {@code null}
   */
  NettyContextCloseable(NettyContext context) {
    this.context = Objects.requireNonNull(context, "context must not be null");
  }

  /**
   * Returns the address that the {@link NettyContext} is listening on.
   *
   * @return the address that the {@link NettyContext} is listening on
   * @see NettyContext#address()
   */
  public InetSocketAddress address() {
    return context.address();
  }

  @Override
  public void dispose() {
    context.dispose();
  }

  @Override
  public boolean isDisposed() {
    return context.isDisposed();
  }

  @Override
  public Mono<Void> onClose() {
    return context.onClose();
  }
}
