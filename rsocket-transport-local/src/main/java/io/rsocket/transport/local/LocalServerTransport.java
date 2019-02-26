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

import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Closeable;
import io.rsocket.DuplexConnection;
import io.rsocket.fragmentation.FragmentationDuplexConnection;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.util.annotation.Nullable;

/**
 * An implementation of {@link ServerTransport} that connects to a {@link ClientTransport} in the
 * same JVM.
 */
public final class LocalServerTransport implements ServerTransport<Closeable> {

  private static final ConcurrentMap<String, ServerDuplexConnectionAcceptor> registry =
      new ConcurrentHashMap<>();

  private final String name;

  private LocalServerTransport(String name) {
    this.name = name;
  }

  /**
   * Creates an instance.
   *
   * @param name the name of this {@link ServerTransport} that clients will connect to
   * @return a new instance
   * @throws NullPointerException if {@code name} is {@code null}
   */
  public static LocalServerTransport create(String name) {
    Objects.requireNonNull(name, "name must not be null");
    return new LocalServerTransport(name);
  }

  /**
   * Creates an instance with a random name.
   *
   * @return a new instance with a random name
   */
  public static LocalServerTransport createEphemeral() {
    return create(UUID.randomUUID().toString());
  }

  /**
   * Remove an instance from the JVM registry.
   *
   * @param name the local transport instance to free
   * @throws NullPointerException if {@code name} is {@code null}
   */
  public static void dispose(String name) {
    Objects.requireNonNull(name, "name must not be null");
    registry.remove(name);
  }

  /**
   * Retrieves an instance of {@link ServerDuplexConnectionAcceptor} based on the name of its {@code
   * LocalServerTransport}. Returns {@code null} if that server is not registered.
   *
   * @param name the name of the server to retrieve
   * @return the server if it has been registered, {@code null} otherwise
   * @throws NullPointerException if {@code name} is {@code null}
   */
  static @Nullable ServerDuplexConnectionAcceptor findServer(String name) {
    Objects.requireNonNull(name, "name must not be null");

    return registry.get(name);
  }

  /**
   * Returns a new {@link LocalClientTransport} that is connected to this {@code
   * LocalServerTransport}.
   *
   * @return a new {@link LocalClientTransport} that is connected to this {@code
   *     LocalServerTransport}
   */
  public LocalClientTransport clientTransport() {
    return LocalClientTransport.create(name);
  }

  @Override
  public Mono<Closeable> start(ConnectionAcceptor acceptor, int mtu) {
    Objects.requireNonNull(acceptor, "acceptor must not be null");

    return Mono.create(
        sink -> {
          ServerDuplexConnectionAcceptor serverDuplexConnectionAcceptor =
              new ServerDuplexConnectionAcceptor(name, acceptor, mtu);

          if (registry.putIfAbsent(name, serverDuplexConnectionAcceptor) != null) {
            throw new IllegalStateException("name already registered: " + name);
          }

          sink.success(serverDuplexConnectionAcceptor);
        });
  }

  /**
   * Returns the name of this instance.
   *
   * @return the name of this instance
   */
  String getName() {
    return name;
  }

  /**
   * A {@link Consumer} of {@link DuplexConnection} that is called when a server has been created.
   */
  static class ServerDuplexConnectionAcceptor implements Consumer<DuplexConnection>, Closeable {

    private final ConnectionAcceptor acceptor;

    private final LocalSocketAddress address;

    private final MonoProcessor<Void> onClose = MonoProcessor.create();

    private final int mtu;

    /**
     * Creates a new instance
     *
     * @param name the name of the server
     * @param acceptor the {@link ConnectionAcceptor} to call when the server has been created
     * @throws NullPointerException if {@code name} or {@code acceptor} is {@code null}
     */
    ServerDuplexConnectionAcceptor(String name, ConnectionAcceptor acceptor, int mtu) {
      Objects.requireNonNull(name, "name must not be null");

      this.address = new LocalSocketAddress(name);
      this.acceptor = Objects.requireNonNull(acceptor, "acceptor must not be null");
      this.mtu = mtu;
    }

    @Override
    public void accept(DuplexConnection duplexConnection) {
      Objects.requireNonNull(duplexConnection, "duplexConnection must not be null");

      if (mtu > 0) {
        duplexConnection =
            new FragmentationDuplexConnection(
                duplexConnection, ByteBufAllocator.DEFAULT, mtu, false);
      }

      acceptor.apply(duplexConnection).subscribe();
    }

    @Override
    public void dispose() {
      if (!registry.remove(address.getName(), this)) {
        throw new AssertionError();
      }

      onClose.onComplete();
    }

    @Override
    public boolean isDisposed() {
      return onClose.isDisposed();
    }

    @Override
    public Mono<Void> onClose() {
      return onClose;
    }
  }
}
