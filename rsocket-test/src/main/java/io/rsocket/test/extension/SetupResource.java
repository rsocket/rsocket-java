/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.test.extension;

import io.rsocket.Closeable;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.test.TestRSocket;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import reactor.core.publisher.Mono;

public abstract class SetupResource<T, S extends Closeable> implements Closeable {
  private final S server;
  private final RSocket client;

  public SetupResource(
      Supplier<T> addressSupplier,
      BiFunction<T, S, ClientTransport> clientTransportSupplier,
      Function<T, ServerTransport<S>> serverTransportSupplier) {

    final T address = addressSupplier.get();
    server = RSocketFactory.receive()
        .acceptor((setup, sendingSocket) -> Mono.just(new TestRSocket()))
        .transport(serverTransportSupplier.apply(address))
        .start()
        .block();
    client = RSocketFactory.connect()
        .transport(clientTransportSupplier.apply(address, server))
        .start()
        .doOnError(Throwable::printStackTrace)
        .block();
  }

  public RSocket getRSocket() {
    return client;
  }

  @Override
  public Mono<Void> close() {
    return server.close();
  }

  @Override
  public Mono<Void> onClose() {
    return server.onClose();
  }
}
