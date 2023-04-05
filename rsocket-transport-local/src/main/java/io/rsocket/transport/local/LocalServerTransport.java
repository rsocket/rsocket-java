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

import io.rsocket.Closeable;
import io.rsocket.DuplexConnection;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.annotation.Nullable;

/**
 * An implementation of {@link ServerTransport} that connects to a {@link ClientTransport} in the
 * same JVM.
 */
public final class LocalServerTransport implements ServerTransport<Closeable> {

  private static final ConcurrentMap<String, ServerCloseableAcceptor> registry =
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
    ServerCloseableAcceptor sca = registry.remove(name);
    if (sca != null) {
      sca.dispose();
    }
  }

  /**
   * Retrieves an instance of {@link ConnectionAcceptor} based on the name of its {@code
   * LocalServerTransport}. Returns {@code null} if that server is not registered.
   *
   * @param name the name of the server to retrieve
   * @return the server if it has been registered, {@code null} otherwise
   * @throws NullPointerException if {@code name} is {@code null}
   */
  static @Nullable ConnectionAcceptor findServer(String name) {
    Objects.requireNonNull(name, "name must not be null");

    return registry.get(name);
  }

  /** Return the name associated with this local server instance. */
  String getName() {
    return name;
  }

  /**
   * Return a new {@link LocalClientTransport} connected to this {@code LocalServerTransport}
   * through its {@link #getName()}.
   */
  public LocalClientTransport clientTransport() {
    return LocalClientTransport.create(name);
  }

  @Override
  public Mono<Closeable> start(ConnectionAcceptor acceptor) {
    Objects.requireNonNull(acceptor, "acceptor must not be null");
    return Mono.create(
        sink -> {
          ServerCloseableAcceptor closeable = new ServerCloseableAcceptor(name, acceptor);
          if (registry.putIfAbsent(name, closeable) != null) {
            sink.error(new IllegalStateException("name already registered: " + name));
          }
          sink.success(closeable);
        });
  }

  @SuppressWarnings({"ReactorTransformationOnMonoVoid", "CallingSubscribeInNonBlockingScope"})
  static class ServerCloseableAcceptor implements ConnectionAcceptor, Closeable {

    private final LocalSocketAddress address;

    private final ConnectionAcceptor acceptor;

    private final Set<DuplexConnection> activeConnections = ConcurrentHashMap.newKeySet();

    private final Sinks.Empty<Void> onClose = Sinks.unsafe().empty();

    ServerCloseableAcceptor(String name, ConnectionAcceptor acceptor) {
      Objects.requireNonNull(name, "name must not be null");
      this.address = new LocalSocketAddress(name);
      this.acceptor = acceptor;
    }

    @Override
    public Mono<Void> apply(DuplexConnection duplexConnection) {
      activeConnections.add(duplexConnection);
      duplexConnection
          .onClose()
          .doFinally(__ -> activeConnections.remove(duplexConnection))
          .subscribe();
      return acceptor.apply(duplexConnection);
    }

    @Override
    public void dispose() {
      if (!registry.remove(address.getName(), this)) {
        // already disposed
        return;
      }

      Mono.whenDelayError(
              activeConnections
                  .stream()
                  .peek(DuplexConnection::dispose)
                  .map(DuplexConnection::onClose)
                  .collect(Collectors.toList()))
          .subscribe(null, onClose::tryEmitError, onClose::tryEmitEmpty);
    }

    @Override
    @SuppressWarnings("ConstantConditions")
    public boolean isDisposed() {
      return onClose.scan(Scannable.Attr.TERMINATED) || onClose.scan(Scannable.Attr.CANCELLED);
    }

    @Override
    public Mono<Void> onClose() {
      return onClose.asMono();
    }
  }
}
