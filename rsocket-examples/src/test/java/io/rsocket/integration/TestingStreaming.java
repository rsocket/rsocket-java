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

package io.rsocket.integration;

import io.rsocket.*;
import io.rsocket.exceptions.ApplicationErrorException;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.local.LocalClientTransport;
import io.rsocket.transport.local.LocalServerTransport;
import io.rsocket.util.DefaultPayload;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class TestingStreaming {
  private Supplier<ServerTransport<? extends Closeable>> serverSupplier =
      () -> LocalServerTransport.create("test");

  private Supplier<ClientTransport> clientSupplier = () -> LocalClientTransport.create("test");

  @Test(expected = ApplicationErrorException.class)
  public void testRangeButThrowException() {
    Closeable server = null;
    try {
      server =
          RSocketFactory.receive()
              .errorConsumer(Throwable::printStackTrace)
              .acceptor(
                  (connectionSetupPayload, rSocket) -> {
                    AbstractRSocket abstractRSocket =
                        new AbstractRSocket() {
                          @Override
                          public double availability() {
                            return 1.0;
                          }

                          @Override
                          public Flux<Payload> requestStream(Payload payload) {
                            return Flux.range(1, 1000)
                                .doOnNext(
                                    i -> {
                                      if (i > 3) {
                                        throw new RuntimeException("BOOM!");
                                      }
                                    })
                                .map(l -> DefaultPayload.create("l -> " + l))
                                .cast(Payload.class);
                          }
                        };

                    return Mono.just(abstractRSocket);
                  })
              .transport(serverSupplier.get())
              .start()
              .block();

      Flux.range(1, 6).flatMap(i -> consumer("connection number -> " + i)).blockLast();
      System.out.println("here");

    } finally {
      server.dispose();
    }
  }

  @Test
  public void testRangeOfConsumers() {
    Closeable server = null;
    try {
      server =
          RSocketFactory.receive()
              .errorConsumer(Throwable::printStackTrace)
              .acceptor(
                  (connectionSetupPayload, rSocket) -> {
                    AbstractRSocket abstractRSocket =
                        new AbstractRSocket() {
                          @Override
                          public double availability() {
                            return 1.0;
                          }

                          @Override
                          public Flux<Payload> requestStream(Payload payload) {
                            return Flux.range(1, 1000)
                                .map(l -> DefaultPayload.create("l -> " + l))
                                .cast(Payload.class);
                          }
                        };

                    return Mono.just(abstractRSocket);
                  })
              .transport(serverSupplier.get())
              .start()
              .block();

      Flux.range(1, 6).flatMap(i -> consumer("connection number -> " + i)).blockLast();
      System.out.println("here");

    } finally {
      server.dispose();
    }
  }

  private Flux<Payload> consumer(String s) {
    return RSocketFactory.connect()
        .errorConsumer(Throwable::printStackTrace)
        .transport(clientSupplier)
        .start()
        .flatMapMany(
            rSocket -> {
              AtomicInteger count = new AtomicInteger();
              return Flux.range(1, 100)
                  .flatMap(
                      i -> rSocket.requestStream(DefaultPayload.create("i -> " + i)).take(100), 1);
            });
  }

  @Test
  public void testSingleConsumer() {
    Closeable server = null;

    try {
      server =
          RSocketFactory.receive()
              .acceptor(
                  (connectionSetupPayload, rSocket) -> {
                    AbstractRSocket abstractRSocket =
                        new AbstractRSocket() {
                          @Override
                          public double availability() {
                            return 1.0;
                          }

                          @Override
                          public Flux<Payload> requestStream(Payload payload) {
                            return Flux.range(1, 10_000)
                                .map(l -> DefaultPayload.create("l -> " + l))
                                .cast(Payload.class);
                          }
                        };

                    return Mono.just(abstractRSocket);
                  })
              .transport(serverSupplier.get())
              .start()
              .block();

      consumer("1").blockLast();

    } finally {
      server.dispose();
    }
  }

  @Test
  public void testFluxOnly() {
    Flux<Long> longFlux = Flux.interval(Duration.ofMillis(1)).onBackpressureDrop();

    Flux.range(1, 60).flatMap(i -> longFlux.take(1000)).blockLast();
  }
}
