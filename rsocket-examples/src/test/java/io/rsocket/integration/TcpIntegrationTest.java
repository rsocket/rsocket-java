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

package io.rsocket.integration;

import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeout;

import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.NettyContextCloseable;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.PayloadImpl;
import io.rsocket.util.RSocketProxy;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

public class TcpIntegrationTest {
  private static final Duration TIMEOUT = ofSeconds(5);

  private static AbstractRSocket handler;

  private static NettyContextCloseable server;

  @BeforeAll
  public static void startup() {
    TcpServerTransport serverTransport = TcpServerTransport.create(0);
    server =
        RSocketFactory.receive()
            .acceptor((setup, sendingSocket) -> Mono.just(new RSocketProxy(handler)))
            .transport(serverTransport)
            .start()
            .block();
  }

  private RSocket buildClient() {
    return RSocketFactory.connect()
        .transport(TcpClientTransport.create(server.address()))
        .start()
        .block();
  }

  @AfterAll
  public static void cleanup() {
    server.close().block();
  }

  @Test
  public void testCompleteWithoutNext() {
    Boolean hasElements = assertTimeout(
        TIMEOUT,
        () -> {
          handler =
              new AbstractRSocket() {
                @Override
                public Flux<Payload> requestStream(Payload payload) {
                  return Flux.empty();
                }
              };
          RSocket client = buildClient();

              return client.requestStream(new PayloadImpl("REQUEST", "META")).log().hasElements().block();
        });
    assertFalse(hasElements);
  }

  @Test
  public void testSingleStream() {
    Payload result = assertTimeout(
        TIMEOUT,
        () -> {
          handler =
              new AbstractRSocket() {
                @Override
                public Flux<Payload> requestStream(Payload payload) {
                  return Flux.just(new PayloadImpl("RESPONSE", "METADATA"));
                }
              };

          RSocket client = buildClient();

          return client.requestStream(new PayloadImpl("REQUEST", "META")).blockLast();
        });
    assertEquals("RESPONSE", result.getDataUtf8());
  }

  @Test
  public void testZeroPayload() {
    Payload result = assertTimeout(
        TIMEOUT,
        () -> {
          handler =
              new AbstractRSocket() {
                @Override
                public Flux<Payload> requestStream(Payload payload) {
                  return Flux.just(PayloadImpl.EMPTY);
                }
              };

          RSocket client = buildClient();

          return client.requestStream(new PayloadImpl("REQUEST", "META")).blockFirst();
        });
    assertEquals("", result.getDataUtf8());
  }

  @Disabled
  @Test
  public void testRequestResponseErrors() {
    Tuple2<Payload,Payload> response = assertTimeout(
        TIMEOUT,
        () -> {
          handler =
              new AbstractRSocket() {
                boolean first = true;

                @Override
                public Mono<Payload> requestResponse(Payload payload) {
                  if (first) {
                    first = false;
                    return Mono.error(new RuntimeException("EX"));
                  } else {
                    return Mono.just(new PayloadImpl("SUCCESS"));
                  }
                }
              };

          RSocket client = buildClient();

          Payload response1 =
              client
                  .requestResponse(new PayloadImpl("REQUEST", "META"))
                  .onErrorReturn(new PayloadImpl("ERROR"))
                  .block();
          Payload response2 =
              client
                  .requestResponse(new PayloadImpl("REQUEST", "META"))
                  .onErrorReturn(new PayloadImpl("ERROR"))
                  .block();
          return Tuples.of(response1, response2);
        });

    assertEquals("ERROR", response.getT1().getDataUtf8());
    assertEquals("SUCCESS", response.getT2().getDataUtf8());
  }

  @Test
  public void testTwoConcurrentStreams() {
    assertTimeout(
        TIMEOUT,
        () -> {
          ConcurrentHashMap<String, UnicastProcessor<Payload>> map = new ConcurrentHashMap<>();
          UnicastProcessor<Payload> processor1 = UnicastProcessor.create();
          map.put("REQUEST1", processor1);
          UnicastProcessor<Payload> processor2 = UnicastProcessor.create();
          map.put("REQUEST2", processor2);

          handler =
              new AbstractRSocket() {
                @Override
                public Flux<Payload> requestStream(Payload payload) {
                  return map.get(payload.getDataUtf8());
                }
              };

          RSocket client = buildClient();

          Flux<Payload> response1 = client.requestStream(new PayloadImpl("REQUEST1"));
          Flux<Payload> response2 = client.requestStream(new PayloadImpl("REQUEST2"));

          CountDownLatch nextCountdown = new CountDownLatch(2);
          CountDownLatch completeCountdown = new CountDownLatch(2);

          response1
              .subscribeOn(Schedulers.newSingle("1"))
              .subscribe(c -> nextCountdown.countDown(), t -> {
              }, completeCountdown::countDown);

          response2
              .subscribeOn(Schedulers.newSingle("2"))
              .subscribe(c -> nextCountdown.countDown(), t -> {
              }, completeCountdown::countDown);

          processor1.onNext(new PayloadImpl("RESPONSE1A"));
          processor2.onNext(new PayloadImpl("RESPONSE2A"));

          nextCountdown.await();

          processor1.onComplete();
          processor2.onComplete();

          completeCountdown.await();
        });
  }
}
