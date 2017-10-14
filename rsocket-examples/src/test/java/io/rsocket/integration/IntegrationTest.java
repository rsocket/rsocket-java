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

package io.rsocket.integration;

import static java.time.Duration.ofSeconds;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.plugins.DuplexConnectionInterceptor;
import io.rsocket.plugins.RSocketInterceptor;
import io.rsocket.test.TestSubscriber;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.NettyContextCloseable;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.PayloadImpl;
import io.rsocket.util.RSocketProxy;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class IntegrationTest {
  private static final Duration TIMEOUT = ofSeconds(5);

  private static NettyContextCloseable server;
  private static RSocket client;
  private static AtomicInteger requestCount;
  private static CountDownLatch disconnectionCounter;
  private static volatile boolean calledClient = false;
  private static volatile boolean calledServer = false;
  private static volatile boolean calledFrame = false;

  private static final RSocketInterceptor clientPlugin;
  private static final RSocketInterceptor serverPlugin;
  private static final DuplexConnectionInterceptor connectionPlugin;

  static {
    clientPlugin =
        reactiveSocket ->
            new RSocketProxy(reactiveSocket) {
              @Override
              public Mono<Payload> requestResponse(Payload payload) {
                calledClient = true;
                return reactiveSocket.requestResponse(payload);
              }
            };

    serverPlugin =
        reactiveSocket ->
            new RSocketProxy(reactiveSocket) {
              @Override
              public Mono<Payload> requestResponse(Payload payload) {
                calledServer = true;
                return reactiveSocket.requestResponse(payload);
              }
            };

    connectionPlugin =
        (type, connection) -> {
          calledFrame = true;
          return connection;
        };
  }

  @BeforeEach
  public void startup() {
    requestCount = new AtomicInteger();
    disconnectionCounter = new CountDownLatch(1);

    TcpServerTransport serverTransport = TcpServerTransport.create(0);

    server =
        RSocketFactory.receive()
            .addServerPlugin(serverPlugin)
            .addConnectionPlugin(connectionPlugin)
            .acceptor(
                (setup, sendingSocket) -> {
                  sendingSocket
                      .onClose()
                      .doFinally(signalType -> disconnectionCounter.countDown())
                      .subscribe();

                  return Mono.just(
                      new AbstractRSocket() {
                        @Override
                        public Mono<Payload> requestResponse(Payload payload) {
                          return Mono.<Payload>just(new PayloadImpl("RESPONSE", "METADATA"))
                              .doOnSubscribe(s -> requestCount.incrementAndGet());
                        }

                        @Override
                        public Flux<Payload> requestStream(Payload payload) {
                          return Flux.range(1, 10_000).map(i -> new PayloadImpl("data -> " + i));
                        }
                      });
                })
            .transport(serverTransport)
            .start()
            .block();

    client =
        RSocketFactory.connect()
            .addClientPlugin(clientPlugin)
            .addConnectionPlugin(connectionPlugin)
            .transport(TcpClientTransport.create(server.address()))
            .start()
            .block();
  }

  @AfterEach
  public void teardown() {
    server.close().block();
  }

  @Test
  public void testRequest() {
    assertTimeout(
        TIMEOUT,
        () ->  client.requestResponse(new PayloadImpl("REQUEST", "META")).block());
    assertThat("Server did not see the request.", requestCount.get(), is(1));
    assertTrue(calledClient);
    assertTrue(calledServer);
    assertTrue(calledFrame);
  }

  @Test
  public void testStream() {
    Subscriber<Payload> subscriber = assertTimeout(
        TIMEOUT,
        () -> {
          Subscriber<Payload> s = TestSubscriber.createCancelling();
          client.requestStream(new PayloadImpl("start")).subscribe(s);
          return s;
        });
    verify(subscriber).onSubscribe(any());
    verifyNoMoreInteractions(subscriber);
  }

  @Test
  public void testClose() {
    assertTimeout(
        TIMEOUT,
        () -> {
          client.close().block();
          disconnectionCounter.await();
        });
  }
}
