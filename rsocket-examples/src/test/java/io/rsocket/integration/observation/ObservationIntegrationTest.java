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

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.core.tck.MeterRegistryAssert;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.test.SampleTestRunner;
import io.micrometer.tracing.test.reporter.BuildingBlocks;
import io.micrometer.tracing.test.simple.SpansAssert;
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
import java.time.Duration;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ObservationIntegrationTest extends SampleTestRunner {
  private static final MeterRegistry registry = new SimpleMeterRegistry();
  private static final ObservationRegistry observationRegistry = ObservationRegistry.create();

  static {
    observationRegistry
        .observationConfig()
        .observationHandler(new DefaultMeterObservationHandler(registry));
  }

  private final RSocketInterceptor requesterInterceptor;
  private final RSocketInterceptor responderInterceptor;

  ObservationIntegrationTest() {
    super(SampleRunnerConfig.builder().build());
    requesterInterceptor =
        reactiveSocket -> new ObservationRequesterRSocketProxy(reactiveSocket, observationRegistry);

    responderInterceptor =
        reactiveSocket -> new ObservationResponderRSocketProxy(reactiveSocket, observationRegistry);
  }

  private CloseableChannel server;
  private RSocket client;
  private AtomicInteger counter;

  @Override
  public BiConsumer<BuildingBlocks, Deque<ObservationHandler<? extends Observation.Context>>>
      customizeObservationHandlers() {
    return (buildingBlocks, observationHandlers) -> {
      observationHandlers.addFirst(
          new RSocketRequesterTracingObservationHandler(
              buildingBlocks.getTracer(),
              buildingBlocks.getPropagator(),
              new ByteBufSetter(),
              false));
      observationHandlers.addFirst(
          new RSocketResponderTracingObservationHandler(
              buildingBlocks.getTracer(),
              buildingBlocks.getPropagator(),
              new ByteBufGetter(),
              false));
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
    Awaitility.await().atMost(Duration.ofSeconds(50)).until(() -> counter.get() == 1);
    assertThat(counter).as("Server did not see the request.").hasValue(1);
  }

  @Override
  public SampleTestRunnerConsumer yourCode() {
    return (bb, meterRegistry) -> {
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

      // @formatter:off
      SpansAssert.assertThat(bb.getFinishedSpans())
          .haveSameTraceId()
          // "request_*" + "handle" x 4
          .hasNumberOfSpansEqualTo(8)
          .hasNumberOfSpansWithNameEqualTo("handle", 4)
          .forAllSpansWithNameEqualTo("handle", span -> span.hasTagWithKey("rsocket.request-type"))
          .hasASpanWithNameIgnoreCase("request_stream")
          .thenASpanWithNameEqualToIgnoreCase("request_stream")
          .hasTag("rsocket.request-type", "REQUEST_STREAM")
          .backToSpans()
          .hasASpanWithNameIgnoreCase("request_channel")
          .thenASpanWithNameEqualToIgnoreCase("request_channel")
          .hasTag("rsocket.request-type", "REQUEST_CHANNEL")
          .backToSpans()
          .hasASpanWithNameIgnoreCase("request_fnf")
          .thenASpanWithNameEqualToIgnoreCase("request_fnf")
          .hasTag("rsocket.request-type", "REQUEST_FNF")
          .backToSpans()
          .hasASpanWithNameIgnoreCase("request_response")
          .thenASpanWithNameEqualToIgnoreCase("request_response")
          .hasTag("rsocket.request-type", "REQUEST_RESPONSE");

      MeterRegistryAssert.assertThat(registry)
          .hasTimerWithNameAndTags(
              "rsocket.response",
              Tags.of(Tag.of("error", "none"), Tag.of("rsocket.request-type", "REQUEST_RESPONSE")))
          .hasTimerWithNameAndTags(
              "rsocket.fnf",
              Tags.of(Tag.of("error", "none"), Tag.of("rsocket.request-type", "REQUEST_FNF")))
          .hasTimerWithNameAndTags(
              "rsocket.request",
              Tags.of(Tag.of("error", "none"), Tag.of("rsocket.request-type", "REQUEST_RESPONSE")))
          .hasTimerWithNameAndTags(
              "rsocket.channel",
              Tags.of(Tag.of("error", "none"), Tag.of("rsocket.request-type", "REQUEST_CHANNEL")))
          .hasTimerWithNameAndTags(
              "rsocket.stream",
              Tags.of(Tag.of("error", "none"), Tag.of("rsocket.request-type", "REQUEST_STREAM")));
      // @formatter:on
    };
  }

  @Override
  protected MeterRegistry getMeterRegistry() {
    return registry;
  }

  @Override
  protected ObservationRegistry getObservationRegistry() {
    return observationRegistry;
  }
}
