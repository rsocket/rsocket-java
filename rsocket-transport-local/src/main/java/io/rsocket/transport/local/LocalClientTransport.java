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

import io.rsocket.DuplexConnection;
import io.rsocket.Frame;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.local.LocalServerTransport.ServerDuplexConnectionAcceptor;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;

public class LocalClientTransport implements ClientTransport {
  private final String name;

  LocalClientTransport(String name) {
    this.name = name;
  }

  public static LocalClientTransport create(String name) {
    return new LocalClientTransport(name);
  }

  @Override
  public Mono<DuplexConnection> connect() {
    return Mono.defer(
        () -> {
          ServerDuplexConnectionAcceptor server = LocalServerTransport.findServer(name);
          if (server != null) {
            final UnicastProcessor<Frame> in = UnicastProcessor.create();
            final UnicastProcessor<Frame> out = UnicastProcessor.create();
            final MonoProcessor<Void> closeNotifier = MonoProcessor.create();
            server.accept(new LocalDuplexConnection(out, in, closeNotifier));
            DuplexConnection client = new LocalDuplexConnection(in, out, closeNotifier);
            return Mono.just(client);
          }
          return Mono.error(new IllegalArgumentException("Could not find server: " + name));
        });
  }
}
