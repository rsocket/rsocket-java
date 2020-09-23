/*
 * Copyright 2015-2020 the original author or authors.
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

import io.rsocket.Closeable;
import io.rsocket.Payload;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.exceptions.ApplicationErrorException;
import io.rsocket.transport.local.LocalClientTransport;
import io.rsocket.transport.local.LocalServerTransport;
import io.rsocket.util.DefaultPayload;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import reactor.core.publisher.Flux;

public class TestingStreaming {
  LocalServerTransport serverTransport = LocalServerTransport.create("test");

  @Test(expected = ApplicationErrorException.class)
  public void testRangeButThrowException() {
    Closeable server = null;
    try {
      server =
          RSocketServer.create(
                  SocketAcceptor.forRequestStream(
                      payload ->
                          Flux.range(1, 1000)
                              .doOnNext(
                                  i -> {
                                    if (i > 3) {
                                      throw new RuntimeException("BOOM!");
                                    }
                                  })
                              .map(l -> DefaultPayload.create("l -> " + l))
                              .cast(Payload.class)))
              .bind(serverTransport)
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
          RSocketServer.create(
                  SocketAcceptor.forRequestStream(
                      payload ->
                          Flux.range(1, 1000)
                              .map(l -> DefaultPayload.create("l -> " + l))
                              .cast(Payload.class)))
              .bind(serverTransport)
              .block();

      Flux.range(1, 6).flatMap(i -> consumer("connection number -> " + i)).blockLast();
      System.out.println("here");

    } finally {
      server.dispose();
    }
  }

  private Flux<Payload> consumer(String s) {
    return RSocketConnector.connectWith(LocalClientTransport.create("test"))
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
          RSocketServer.create(
                  SocketAcceptor.forRequestStream(
                      payload ->
                          Flux.range(1, 10_000)
                              .map(l -> DefaultPayload.create("l -> " + l))
                              .cast(Payload.class)))
              .bind(serverTransport)
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
