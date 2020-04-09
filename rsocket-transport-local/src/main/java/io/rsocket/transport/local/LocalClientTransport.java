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

package io.rsocket.transport.local;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.DuplexConnection;
import io.rsocket.fragmentation.FragmentationDuplexConnection;
import io.rsocket.fragmentation.ReassemblyDuplexConnection;
import io.rsocket.internal.UnboundedProcessor;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.local.LocalServerTransport.ServerDuplexConnectionAcceptor;
import java.util.Objects;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

/**
 * An implementation of {@link ClientTransport} that connects to a {@link ServerTransport} in the
 * same JVM.
 */
public final class LocalClientTransport implements ClientTransport {

  private final String name;

  private LocalClientTransport(String name) {
    this.name = name;
  }

  /**
   * Creates a new instance.
   *
   * @param name the name of the {@link ServerTransport} instance to connect to
   * @return a new instance
   * @throws NullPointerException if {@code name} is {@code null}
   */
  public static LocalClientTransport create(String name) {
    Objects.requireNonNull(name, "name must not be null");

    return new LocalClientTransport(name);
  }

  private Mono<DuplexConnection> connect() {
    return Mono.defer(
        () -> {
          ServerDuplexConnectionAcceptor server = LocalServerTransport.findServer(name);
          if (server == null) {
            return Mono.error(new IllegalArgumentException("Could not find server: " + name));
          }

          UnboundedProcessor<ByteBuf> in = new UnboundedProcessor<>();
          UnboundedProcessor<ByteBuf> out = new UnboundedProcessor<>();
          MonoProcessor<Void> closeNotifier = MonoProcessor.create();

          server.accept(
              new ReassemblyDuplexConnection(
                  new LocalDuplexConnection(out, in, closeNotifier),
                  ByteBufAllocator.DEFAULT,
                  false));

          return Mono.just(
              (DuplexConnection)
                  new ReassemblyDuplexConnection(
                      new LocalDuplexConnection(in, out, closeNotifier),
                      ByteBufAllocator.DEFAULT,
                      false));
        });
  }

  @Override
  public Mono<DuplexConnection> connect(int mtu) {
    Mono<DuplexConnection> isError = FragmentationDuplexConnection.checkMtu(mtu);
    Mono<DuplexConnection> connect = isError != null ? isError : connect();
    if (mtu > 0) {
      return connect.map(
          duplexConnection ->
              new FragmentationDuplexConnection(
                  duplexConnection, ByteBufAllocator.DEFAULT, mtu, false, "client"));
    } else {
      return connect;
    }
  }
}
