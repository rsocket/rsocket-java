/*
 * Copyright 2016 Netflix, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.rsocket.transport.netty.server;

import io.rsocket.Closeable;
import java.net.InetSocketAddress;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.ipc.netty.NettyContext;

/**
 * A {@link Closeable} wrapping a {@link NettyContext}, allowing for close and aware of its address.
 */
public class NettyContextCloseable implements Closeable {
  private NettyContext context;

  private MonoProcessor<Void> onClose;

  NettyContextCloseable(NettyContext context) {
    this.onClose = MonoProcessor.create();
    this.context = context;
  }

  @Override
  public Mono<Void> close() {
    return Mono.empty()
        .doFinally(
            s -> {
              context.dispose();
              onClose.onComplete();
            })
        .then();
  }

  @Override
  public Mono<Void> onClose() {
    return onClose;
  }

  /** @see NettyContext#address() */
  public InetSocketAddress address() {
    return context.address();
  }
}
