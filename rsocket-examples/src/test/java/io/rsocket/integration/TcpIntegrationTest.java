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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import io.rsocket.util.EmptyPayload;
import io.rsocket.util.RSocketProxy;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;
import reactor.core.scheduler.Schedulers;

public class TcpIntegrationTest {
  private AbstractRSocket handler;

  private CloseableChannel server;

  @Before
  public void startup() {
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

  @After
  public void cleanup() {
    server.dispose();
  }

  @Test(timeout = 5_000L)
  public void testCompleteWithoutNext() {
    handler =
        new AbstractRSocket() {
          @Override
          public Flux<Payload> requestStream(Payload payload) {
            return Flux.empty();
          }
        };
    RSocket client = buildClient();
    Boolean hasElements =
        client.requestStream(DefaultPayload.create("REQUEST", "META")).log().hasElements().block();

    assertFalse(hasElements);
  }

  @Test(timeout = 5_000L)
  public void testSingleStream() {
    handler =
        new AbstractRSocket() {
          @Override
          public Flux<Payload> requestStream(Payload payload) {
            return Flux.just(DefaultPayload.create("RESPONSE", "METADATA"));
          }
        };

    RSocket client = buildClient();

    Payload result = client.requestStream(DefaultPayload.create("REQUEST", "META")).blockLast();

    assertEquals("RESPONSE", result.getDataUtf8());
  }

  @Test(timeout = 5_000L)
  public void testZeroPayload() {
    handler =
        new AbstractRSocket() {
          @Override
          public Flux<Payload> requestStream(Payload payload) {
            return Flux.just(EmptyPayload.INSTANCE);
          }
        };

    RSocket client = buildClient();

    Payload result = client.requestStream(DefaultPayload.create("REQUEST", "META")).blockFirst();

    assertEquals("", result.getDataUtf8());
  }

  @Test(timeout = 5_000L)
  public void testRequestResponseErrors() {
    handler =
        new AbstractRSocket() {
          boolean first = true;

          @Override
          public Mono<Payload> requestResponse(Payload payload) {
            if (first) {
              first = false;
              return Mono.error(new RuntimeException("EX"));
            } else {
              return Mono.just(DefaultPayload.create("SUCCESS"));
            }
          }
        };

    RSocket client = buildClient();

    Payload response1 =
        client
            .requestResponse(DefaultPayload.create("REQUEST", "META"))
            .onErrorReturn(DefaultPayload.create("ERROR"))
            .block();
    Payload response2 =
        client
            .requestResponse(DefaultPayload.create("REQUEST", "META"))
            .onErrorReturn(DefaultPayload.create("ERROR"))
            .block();

    assertEquals("ERROR", response1.getDataUtf8());
    assertEquals("SUCCESS", response2.getDataUtf8());
  }

  @Test(timeout = 5_000L)
  public void testTwoConcurrentStreams() throws InterruptedException {
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

    Flux<Payload> response1 = client.requestStream(DefaultPayload.create("REQUEST1"));
    Flux<Payload> response2 = client.requestStream(DefaultPayload.create("REQUEST2"));

    CountDownLatch nextCountdown = new CountDownLatch(2);
    CountDownLatch completeCountdown = new CountDownLatch(2);

    response1
        .subscribeOn(Schedulers.newSingle("1"))
        .subscribe(c -> nextCountdown.countDown(), t -> {}, completeCountdown::countDown);

    response2
        .subscribeOn(Schedulers.newSingle("2"))
        .subscribe(c -> nextCountdown.countDown(), t -> {}, completeCountdown::countDown);

    processor1.onNext(DefaultPayload.create("RESPONSE1A"));
    processor2.onNext(DefaultPayload.create("RESPONSE2A"));

    nextCountdown.await();

    processor1.onComplete();
    processor2.onComplete();

    completeCountdown.await();
  }
}
