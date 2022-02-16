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

package io.rsocket.integration.observation;

import java.time.Duration;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import io.micrometer.api.instrument.MeterRegistry;
import io.micrometer.api.instrument.observation.ObservationHandler;
import io.micrometer.api.instrument.simple.SimpleMeterRegistry;
import io.micrometer.tracing.test.SampleTestRunner;
import io.micrometer.tracing.test.reporter.BuildingBlocks;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.micrometer.observation.ByteBufGetter;
import io.rsocket.micrometer.observation.ByteBufSetter;
import io.rsocket.micrometer.observation.ObservationRequesterRSocketProxy;
import io.rsocket.micrometer.observation.ObservationResponderRSocketProxy;
import io.rsocket.micrometer.observation.RSocketRequesterTracingObservationHandler;
import io.rsocket.micrometer.observation.RSocketResponderTracingObservationHandler;
import io.rsocket.plugins.RSocketInterceptor;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;

public class ObservationIntegrationTest extends SampleTestRunner {
  private static final MeterRegistry registry = new SimpleMeterRegistry()
          .withTimerObservationHandler();

  private final RSocketInterceptor requesterInterceptor;
  private final RSocketInterceptor responderInterceptor;

  ObservationIntegrationTest() {
    super(SampleRunnerConfig.builder().build(), registry);
    requesterInterceptor =
            reactiveSocket ->
                    new ObservationRequesterRSocketProxy(reactiveSocket, registry);

    responderInterceptor =
            reactiveSocket ->
                    new ObservationResponderRSocketProxy(reactiveSocket, registry);
  }

  private CloseableChannel server;
  private RSocket client;
  private AtomicInteger counter;

  @Override
  public BiConsumer<BuildingBlocks, Deque<ObservationHandler>> customizeObservationHandlers() {
    return (buildingBlocks, observationHandlers) -> {
      observationHandlers.addFirst(new RSocketRequesterTracingObservationHandler(buildingBlocks.getTracer(), buildingBlocks.getPropagator(), new ByteBufSetter(), false));
      observationHandlers.addFirst(new RSocketResponderTracingObservationHandler(buildingBlocks.getTracer(), buildingBlocks.getPropagator(), new ByteBufGetter(), false));
    };
  }

  @AfterEach
  public void teardown() {
    if (server != null) {
      server.dispose();
    }
  }

  private void testRequest() {
    counter.set(0);
    client.requestResponse(DefaultPayload.create("REQUEST", "META")).block();
    assertThat(counter).as("Server did not see the request.").hasValue(1);
  }

  private void testStream() {
    counter.set(0);
    client.requestStream(DefaultPayload.create("start")).blockLast();

    assertThat(counter).as("Server did not see the request.").hasValue(1);
  }

  private void testRequestChannel() {
    counter.set(0);
    client.requestChannel(Mono.just(DefaultPayload.create("start"))).blockFirst();
    assertThat(counter).as("Server did not see the request.").hasValue(1);
  }

  private void testFireAndForget() {
    counter.set(0);
    client.fireAndForget(DefaultPayload.create("start")).subscribe();
    Awaitility.await()
              .atMost(Duration.ofSeconds(5))
              .until(() -> counter.get() == 1);
    assertThat(counter).as("Server did not see the request.").hasValue(1);
  }

  @Override
  public SampleTestRunnerConsumer yourCode() {
    return (tracer, meterRegistry) -> {
      counter = new AtomicInteger();
      server =
          RSocketServer.create(
                  (setup, sendingSocket) -> {
                    sendingSocket.onClose().subscribe();

                    return Mono.just(
                        new RSocket() {
                          @Override
                          public Mono<Payload> requestResponse(Payload payload) {
                            payload.release();
                            counter.incrementAndGet();
                            return Mono.just(DefaultPayload.create("RESPONSE", "METADATA"));
                          }

                          @Override
                          public Flux<Payload> requestStream(Payload payload) {
                            payload.release();
                            counter.incrementAndGet();
                            return Flux.range(1, 10_000)
                                .map(i -> DefaultPayload.create("data -> " + i));
                          }

                          @Override
                          public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
                            counter.incrementAndGet();
                            return Flux.from(payloads);
                          }

                          @Override
                          public Mono<Void> fireAndForget(Payload payload) {
                            payload.release();
                            counter.incrementAndGet();
                            return Mono.empty();
                          }
                        });
                  })
              .interceptors(registry -> registry.forResponder(responderInterceptor))
              .bind(TcpServerTransport.create("localhost", 0))
              .block();

      client =
          RSocketConnector.create()
              .interceptors(registry -> registry.forRequester(requesterInterceptor))
              .connect(TcpClientTransport.create(server.address()))
              .block();

      testRequest();

      testStream();

      testRequestChannel();

      testFireAndForget();
    };
  }
}
