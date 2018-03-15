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

package io.rsocket.aeron.server;

import io.rsocket.Closeable;
import io.rsocket.DuplexConnection;
import io.rsocket.aeron.AeronDuplexConnection;
import io.rsocket.aeron.internal.AeronWrapper;
import io.rsocket.aeron.internal.EventLoop;
import io.rsocket.aeron.internal.reactivestreams.AeronChannelServer;
import io.rsocket.aeron.internal.reactivestreams.AeronSocketAddress;
import io.rsocket.transport.ServerTransport;
import reactor.core.publisher.Mono;

/** */
public class AeronServerTransport implements ServerTransport<Closeable> {
  private final AeronWrapper aeronWrapper;
  private final AeronSocketAddress managementSubscriptionSocket;
  private final EventLoop eventLoop;

  private AeronChannelServer aeronChannelServer;

  public AeronServerTransport(
      AeronWrapper aeronWrapper,
      AeronSocketAddress managementSubscriptionSocket,
      EventLoop eventLoop) {
    this.aeronWrapper = aeronWrapper;
    this.managementSubscriptionSocket = managementSubscriptionSocket;
    this.eventLoop = eventLoop;
  }

  @Override
  public Mono<Closeable> start(ConnectionAcceptor acceptor) {
    synchronized (this) {
      if (aeronChannelServer != null) {
        throw new IllegalStateException("server already ready started");
      }

      aeronChannelServer =
          AeronChannelServer.create(
              aeronChannel -> {
                DuplexConnection connection = new AeronDuplexConnection("server", aeronChannel);
                acceptor.apply(connection).subscribe();
              },
              aeronWrapper,
              managementSubscriptionSocket,
              eventLoop);
    }

    return Mono.just(aeronChannelServer.start());
  }
}
