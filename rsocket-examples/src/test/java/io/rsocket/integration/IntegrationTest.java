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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.plugins.DuplexConnectionInterceptor;
import io.rsocket.plugins.RSocketInterceptor;
import io.rsocket.plugins.SocketAcceptorInterceptor;
import io.rsocket.test.TestSubscriber;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import io.rsocket.util.RSocketProxy;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class IntegrationTest {

  private static final RSocketInterceptor requesterInterceptor;
  private static final RSocketInterceptor responderInterceptor;
  private static final SocketAcceptorInterceptor clientAcceptorInterceptor;
  private static final SocketAcceptorInterceptor serverAcceptorInterceptor;
  private static final DuplexConnectionInterceptor connectionInterceptor;

  private static volatile boolean calledRequester = false;
  private static volatile boolean calledResponder = false;
  private static volatile boolean calledClientAcceptor = false;
  private static volatile boolean calledServerAcceptor = false;
  private static volatile boolean calledFrame = false;

  static {
    requesterInterceptor =
        reactiveSocket ->
            new RSocketProxy(reactiveSocket) {
              @Override
              public Mono<Payload> requestResponse(Payload payload) {
                calledRequester = true;
                return reactiveSocket.requestResponse(payload);
              }
            };

    responderInterceptor =
        reactiveSocket ->
            new RSocketProxy(reactiveSocket) {
              @Override
              public Mono<Payload> requestResponse(Payload payload) {
                calledResponder = true;
                return reactiveSocket.requestResponse(payload);
              }
            };

    clientAcceptorInterceptor =
        acceptor ->
            (setup, sendingSocket) -> {
              calledClientAcceptor = true;
              return acceptor.accept(setup, sendingSocket);
            };

    serverAcceptorInterceptor =
        acceptor ->
            (setup, sendingSocket) -> {
              calledServerAcceptor = true;
              return acceptor.accept(setup, sendingSocket);
            };

    connectionInterceptor =
        (type, connection) -> {
          calledFrame = true;
          return connection;
        };
  }

  private CloseableChannel server;
  private RSocket client;
  private AtomicInteger requestCount;
  private CountDownLatch disconnectionCounter;
  private AtomicInteger errorCount;

  @Before
  public void startup() {
    errorCount = new AtomicInteger();
    requestCount = new AtomicInteger();
    disconnectionCounter = new CountDownLatch(1);

    server =
        RSocketServer.create(
                (setup, sendingSocket) -> {
                  sendingSocket
                      .onClose()
                      .doFinally(signalType -> disconnectionCounter.countDown())
                      .subscribe();

                  return Mono.just(
                      new RSocket() {
                        @Override
                        public Mono<Payload> requestResponse(Payload payload) {
                          return Mono.just(DefaultPayload.create("RESPONSE", "METADATA"))
                              .doOnSubscribe(s -> requestCount.incrementAndGet());
                        }

                        @Override
                        public Flux<Payload> requestStream(Payload payload) {
                          return Flux.range(1, 10_000)
                              .map(i -> DefaultPayload.create("data -> " + i));
                        }

                        @Override
                        public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
                          return Flux.from(payloads);
                        }
                      });
                })
            .interceptors(
                registry ->
                    registry
                        .forResponder(responderInterceptor)
                        .forSocketAcceptor(serverAcceptorInterceptor)
                        .forConnection(connectionInterceptor))
            .bind(TcpServerTransport.create("localhost", 0))
            .block();

    client =
        RSocketConnector.create()
            .interceptors(
                registry ->
                    registry
                        .forRequester(requesterInterceptor)
                        .forSocketAcceptor(clientAcceptorInterceptor)
                        .forConnection(connectionInterceptor))
            .connect(TcpClientTransport.create(server.address()))
            .block();
  }

  @After
  public void teardown() {
    server.dispose();
  }

  @Test(timeout = 5_000L)
  public void testRequest() {
    client.requestResponse(DefaultPayload.create("REQUEST", "META")).block();
    assertThat("Server did not see the request.", requestCount.get(), is(1));
    assertTrue(calledRequester);
    assertTrue(calledResponder);
    assertTrue(calledClientAcceptor);
    assertTrue(calledServerAcceptor);
    assertTrue(calledFrame);
  }

  @Test(timeout = 5_000L)
  public void testStream() {
    Subscriber<Payload> subscriber = TestSubscriber.createCancelling();
    client.requestStream(DefaultPayload.create("start")).subscribe(subscriber);

    verify(subscriber).onSubscribe(any());
    verifyNoMoreInteractions(subscriber);
  }

  @Test(timeout = 5_000L)
  public void testClose() throws InterruptedException {
    client.dispose();
    disconnectionCounter.await();
  }

  @Test // (timeout = 5_000L)
  public void testCallRequestWithErrorAndThenRequest() {
    try {
      client.requestChannel(Mono.error(new Throwable())).blockLast();
    } catch (Throwable t) {
    }

    testRequest();
  }
}
