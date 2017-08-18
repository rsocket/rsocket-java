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

package io.rsocket.transport.local;

import io.rsocket.Closeable;
import io.rsocket.DuplexConnection;
import io.rsocket.transport.ServerTransport;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

/**
 * Local within process transport for RSocket.
 */
public class LocalServerTransport implements ServerTransport<Closeable> {
  private static final ConcurrentMap<String, ServerDuplexConnectionAcceptor> registry =
      new ConcurrentHashMap<>();

  static ServerDuplexConnectionAcceptor findServer(String name) {
    return registry.get(name);
  }

  private final String name;

  private LocalServerTransport(String name) {
    this.name = name;
  }

  public static LocalServerTransport create(String name) {
    return new LocalServerTransport(name);
  }

  public LocalClientTransport clientTransport() {
    return LocalClientTransport.create(name);
  }

  @Override
  public Mono<Closeable> start(ConnectionAcceptor acceptor) {
    return Mono.create(
        sink -> {
          ServerDuplexConnectionAcceptor serverDuplexConnectionAcceptor =
              new ServerDuplexConnectionAcceptor(name, acceptor);
          if (registry.putIfAbsent(name, serverDuplexConnectionAcceptor) != null) {
            throw new IllegalStateException("name already registered: " + name);
          }
          sink.success(serverDuplexConnectionAcceptor);
        });
  }

  public static LocalServerTransport createEphemeral() {
      return create(UUID.randomUUID().toString());
  }

  /**
   * Remove an instance from the JVM registry.
   *
   * @param name the local transport instance to free.
   */
  public static void dispose(String name) {
    registry.remove(name);
  }

  public String getName() {
    return name;
  }

  static class ServerDuplexConnectionAcceptor implements Consumer<DuplexConnection>, Closeable {
    private final LocalSocketAddress address;
    private final ConnectionAcceptor acceptor;
    private final MonoProcessor<Void> closeNotifier = MonoProcessor.create();

    public ServerDuplexConnectionAcceptor(String name, ConnectionAcceptor acceptor) {
      this.address = new LocalSocketAddress(name);
      this.acceptor = acceptor;
    }

    @Override
    public void accept(DuplexConnection duplexConnection) {
      acceptor.apply(duplexConnection).subscribe();
    }

    @Override
    public Mono<Void> close() {
      return Mono.defer(
          () -> {
            if (!registry.remove(address.getName(), this)) {
              throw new AssertionError();
            }

            closeNotifier.onComplete();
            return Mono.empty();
          });
    }

    @Override
    public Mono<Void> onClose() {
      return closeNotifier;
    }
  }
}
