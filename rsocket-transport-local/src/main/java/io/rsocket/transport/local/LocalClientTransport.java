/*
 * Copyright 2015-2021 the original author or authors.
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

package io.rsocket.transport.local;

import io.netty.buffer.ByteBufAllocator;
import io.rsocket.DuplexConnection;
import io.rsocket.internal.UnboundedProcessor;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import java.util.Objects;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

/**
 * An implementation of {@link ClientTransport} that connects to a {@link ServerTransport} in the
 * same JVM.
 */
public final class LocalClientTransport implements ClientTransport {

  private final String name;

  private final ByteBufAllocator allocator;

  private LocalClientTransport(String name, ByteBufAllocator allocator) {
    this.name = name;
    this.allocator = allocator;
  }

  /**
   * Creates a new instance.
   *
   * @param name the name of the {@link ClientTransport} instance to connect to
   * @return a new instance
   * @throws NullPointerException if {@code name} is {@code null}
   */
  public static LocalClientTransport create(String name) {
    Objects.requireNonNull(name, "name must not be null");

    return create(name, ByteBufAllocator.DEFAULT);
  }

  /**
   * Creates a new instance.
   *
   * @param name the name of the {@link ClientTransport} instance to connect to
   * @param allocator the allocator used by {@link ClientTransport} instance
   * @return a new instance
   * @throws NullPointerException if {@code name} is {@code null}
   */
  public static LocalClientTransport create(String name, ByteBufAllocator allocator) {
    Objects.requireNonNull(name, "name must not be null");
    Objects.requireNonNull(allocator, "allocator must not be null");

    return new LocalClientTransport(name, allocator);
  }

  @Override
  public Mono<DuplexConnection> connect() {
    return Mono.defer(
        () -> {
          ServerTransport.ConnectionAcceptor server = LocalServerTransport.findServer(name);
          if (server == null) {
            return Mono.error(new IllegalArgumentException("Could not find server: " + name));
          }

          Sinks.One<Object> inSink = Sinks.one();
          Sinks.One<Object> outSink = Sinks.one();
          UnboundedProcessor in = new UnboundedProcessor(inSink::tryEmitEmpty);
          UnboundedProcessor out = new UnboundedProcessor(outSink::tryEmitEmpty);

          Mono<Void> onClose = inSink.asMono().and(outSink.asMono());

          server.apply(new LocalDuplexConnection(name, allocator, out, in, onClose)).subscribe();

          return Mono.<DuplexConnection>just(
              new LocalDuplexConnection(name, allocator, in, out, onClose));
        });
  }
}
