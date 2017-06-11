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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import io.reactivex.subscribers.TestSubscriber;
import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.plugins.DuplexConnectionInterceptor;
import io.rsocket.plugins.RSocketInterceptor;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.NettyContextCloseable;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.PayloadImpl;
import io.rsocket.util.RSocketProxy;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class IntegrationTest {

  private NettyContextCloseable server;
  private RSocket client;
  private AtomicInteger requestCount;
  private CountDownLatch disconnectionCounter;
  public static volatile boolean calledClient = false;
  public static volatile boolean calledServer = false;
  public static volatile boolean calledFrame = false;

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

  @Before
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
            // TODO fix the Types through RSocketFactory.Start
            .cast(NettyContextCloseable.class)
            .block();

    client =
        RSocketFactory.connect()
            .addClientPlugin(clientPlugin)
            .addConnectionPlugin(connectionPlugin)
            .transport(TcpClientTransport.create(server.address()))
            .start()
            .block();
  }

  @After
  public void teardown() {
    server.close().block();
  }

  @Test(timeout = 5_000L)
  public void testRequest() {
    client.requestResponse(new PayloadImpl("REQUEST", "META")).block();
    assertThat("Server did not see the request.", requestCount.get(), is(1));
    assertTrue(calledClient);
    assertTrue(calledServer);
    assertTrue(calledFrame);
  }

  @Test
  public void testStream() throws Exception {
    TestSubscriber subscriber = TestSubscriber.create();
    client.requestStream(new PayloadImpl("start")).subscribe(subscriber);

    subscriber.cancel();
    subscriber.isCancelled();
    subscriber.assertNotComplete();
  }

  @Test(timeout = 5_000L)
  public void testClose() throws ExecutionException, InterruptedException, TimeoutException {
    client.close().block();
    disconnectionCounter.await();
  }
}
