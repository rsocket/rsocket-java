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

package io.rsocket.core;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.exceptions.ApplicationErrorException;
import io.rsocket.exceptions.CustomRSocketException;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.subscriber.AssertSubscriber;
import io.rsocket.lease.RequesterLeaseHandler;
import io.rsocket.lease.ResponderLeaseHandler;
import io.rsocket.test.util.LocalDuplexConnection;
import io.rsocket.test.util.TestSubscriber;
import io.rsocket.util.DefaultPayload;
import io.rsocket.util.EmptyPayload;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.assertj.core.api.Assertions;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.mockito.ArgumentCaptor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

public class RSocketTest {

  @Rule public final SocketRule rule = new SocketRule();

  public static void assertError(String s, String mode, ArrayList<Throwable> errors) {
    for (Throwable t : errors) {
      if (t.toString().equals(s)) {
        return;
      }
    }

    Assert.fail("Expected " + mode + " connection error: " + s + " other errors " + errors.size());
  }

  @Test(timeout = 2_000)
  public void testRequestReplyNoError() {
    StepVerifier.create(rule.crs.requestResponse(DefaultPayload.create("hello")))
        .expectNextCount(1)
        .expectComplete()
        .verify();
  }

  @Test(timeout = 2000)
  public void testHandlerEmitsError() {
    rule.setRequestAcceptor(
        new AbstractRSocket() {
          @Override
          public Mono<Payload> requestResponse(Payload payload) {
            return Mono.error(new NullPointerException("Deliberate exception."));
          }
        });
    Subscriber<Payload> subscriber = TestSubscriber.create();
    rule.crs.requestResponse(EmptyPayload.INSTANCE).subscribe(subscriber);
    verify(subscriber).onError(any(ApplicationErrorException.class));

    // Client sees error through normal API
    rule.assertNoClientErrors();

    rule.assertServerError("java.lang.NullPointerException: Deliberate exception.");
  }

  @Test(timeout = 2000)
  public void testHandlerEmitsCustomError() {
    rule.setRequestAcceptor(
        new AbstractRSocket() {
          @Override
          public Mono<Payload> requestResponse(Payload payload) {
            return Mono.error(
                new CustomRSocketException(0x00000501, "Deliberate Custom exception."));
          }
        });
    Subscriber<Payload> subscriber = TestSubscriber.create();
    rule.crs.requestResponse(EmptyPayload.INSTANCE).subscribe(subscriber);
    ArgumentCaptor<CustomRSocketException> customRSocketExceptionArgumentCaptor =
        ArgumentCaptor.forClass(CustomRSocketException.class);
    verify(subscriber).onError(customRSocketExceptionArgumentCaptor.capture());

    Assert.assertEquals(
        "Deliberate Custom exception.",
        customRSocketExceptionArgumentCaptor.getValue().getMessage());
    Assert.assertEquals(0x00000501, customRSocketExceptionArgumentCaptor.getValue().errorCode());

    // Client sees error through normal API
    rule.assertNoClientErrors();

    rule.assertServerError("CustomRSocketException (0x501): Deliberate Custom exception.");
  }

  @Test(timeout = 2000)
  public void testRequestPropagatesCorrectlyForRequestChannel() {
    rule.setRequestAcceptor(
        new AbstractRSocket() {
          @Override
          public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
            return Flux.from(payloads)
                // specifically limits request to 3 in order to prevent 256 request from limitRate
                // hidden on the responder side
                .limitRequest(3);
          }
        });

    Flux.range(0, 3)
        .map(i -> DefaultPayload.create("" + i))
        .as(rule.crs::requestChannel)
        .as(publisher -> StepVerifier.create(publisher, 3))
        .expectSubscription()
        .expectNextCount(3)
        .expectComplete()
        .verify(Duration.ofMillis(5000));

    rule.assertNoClientErrors();
    rule.assertNoServerErrors();
  }

  @Test(timeout = 2000)
  public void testStream() throws Exception {
    Flux<Payload> responses = rule.crs.requestStream(DefaultPayload.create("Payload In"));
    StepVerifier.create(responses).expectNextCount(10).expectComplete().verify();
  }

  @Test(timeout = 2000)
  public void testChannel() throws Exception {
    Flux<Payload> requests =
        Flux.range(0, 10).map(i -> DefaultPayload.create("streaming in -> " + i));
    Flux<Payload> responses = rule.crs.requestChannel(requests);
    StepVerifier.create(responses).expectNextCount(10).expectComplete().verify();
  }

  @Test(timeout = 2000)
  public void testErrorPropagatesCorrectly() {
    AtomicReference<Throwable> error = new AtomicReference<>();
    rule.setRequestAcceptor(
        new AbstractRSocket() {
          @Override
          public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
            return Flux.from(payloads).doOnError(error::set);
          }
        });
    Flux<Payload> requests = Flux.error(new RuntimeException("test"));
    Flux<Payload> responses = rule.crs.requestChannel(requests);
    StepVerifier.create(responses).expectErrorMessage("test").verify();
    Assertions.assertThat(error.get()).isNull();
  }

  @Test
  public void requestChannelIndependentTerminationOfUpstreamAndDownstreamTest1() {
    TestPublisher<Payload> outerPublisher = TestPublisher.create();
    AssertSubscriber<Payload> outerAssertSubscriber = new AssertSubscriber<>(0);

    AssertSubscriber<Payload> innerAssertSubscriber = new AssertSubscriber<>(0);
    TestPublisher<Payload> innerPublisher = TestPublisher.create();

    initRequestChannelCase(
        outerPublisher, outerAssertSubscriber, innerPublisher, innerAssertSubscriber);

    nextFromOuterPublisher(outerPublisher, innerAssertSubscriber);

    completeFromOuterPublisher(outerPublisher, innerAssertSubscriber);

    nextFromInnerPublisher(innerPublisher, outerAssertSubscriber);

    completeFromInnerPublisher(innerPublisher, outerAssertSubscriber);
  }

  @Test
  public void requestChannelIndependentTerminationOfUpstreamAndDownstreamTest2() {
    TestPublisher<Payload> outerPublisher = TestPublisher.create();
    AssertSubscriber<Payload> outerAssertSubscriber = new AssertSubscriber<>(0);

    AssertSubscriber<Payload> innerAssertSubscriber = new AssertSubscriber<>(0);
    TestPublisher<Payload> innerPublisher = TestPublisher.create();

    initRequestChannelCase(
        outerPublisher, outerAssertSubscriber, innerPublisher, innerAssertSubscriber);

    nextFromInnerPublisher(innerPublisher, outerAssertSubscriber);

    completeFromInnerPublisher(innerPublisher, outerAssertSubscriber);

    nextFromOuterPublisher(outerPublisher, innerAssertSubscriber);

    completeFromOuterPublisher(outerPublisher, innerAssertSubscriber);
  }

  @Test
  public void requestChannelIndependentTerminationOfUpstreamAndDownstreamTest3() {
    TestPublisher<Payload> outerPublisher = TestPublisher.create();
    AssertSubscriber<Payload> outerAssertSubscriber = new AssertSubscriber<>(0);

    AssertSubscriber<Payload> innerAssertSubscriber = new AssertSubscriber<>(0);
    TestPublisher<Payload> innerPublisher = TestPublisher.create();

    initRequestChannelCase(
        outerPublisher, outerAssertSubscriber, innerPublisher, innerAssertSubscriber);

    nextFromOuterPublisher(outerPublisher, innerAssertSubscriber);

    cancelFromInnerSubscriber(outerPublisher, innerAssertSubscriber);

    nextFromInnerPublisher(innerPublisher, outerAssertSubscriber);

    completeFromInnerPublisher(innerPublisher, outerAssertSubscriber);
  }

  @Test
  public void requestChannelIndependentTerminationOfUpstreamAndDownstreamTest4() {
    TestPublisher<Payload> outerPublisher = TestPublisher.create();
    AssertSubscriber<Payload> outerAssertSubscriber = new AssertSubscriber<>(0);

    AssertSubscriber<Payload> innerAssertSubscriber = new AssertSubscriber<>(0);
    TestPublisher<Payload> innerPublisher = TestPublisher.create();

    initRequestChannelCase(
        outerPublisher, outerAssertSubscriber, innerPublisher, innerAssertSubscriber);

    nextFromInnerPublisher(innerPublisher, outerAssertSubscriber);

    completeFromInnerPublisher(innerPublisher, outerAssertSubscriber);

    nextFromOuterPublisher(outerPublisher, innerAssertSubscriber);

    cancelFromInnerSubscriber(outerPublisher, innerAssertSubscriber);
  }

  @Test
  public void requestChannelIndependentTerminationOfUpstreamAndDownstreamTest5() {
    TestPublisher<Payload> outerPublisher = TestPublisher.create();
    AssertSubscriber<Payload> outerAssertSubscriber = new AssertSubscriber<>(0);

    AssertSubscriber<Payload> innerAssertSubscriber = new AssertSubscriber<>(0);
    TestPublisher<Payload> innerPublisher = TestPublisher.create();

    initRequestChannelCase(
        outerPublisher, outerAssertSubscriber, innerPublisher, innerAssertSubscriber);

    nextFromInnerPublisher(innerPublisher, outerAssertSubscriber);

    nextFromOuterPublisher(outerPublisher, innerAssertSubscriber);

    // ensures both sides are terminated
    cancelFromOuterSubscriber(
        outerPublisher, outerAssertSubscriber, innerPublisher, innerAssertSubscriber);
  }

  void initRequestChannelCase(
      TestPublisher<Payload> outerPublisher,
      AssertSubscriber<Payload> outerAssertSubscriber,
      TestPublisher<Payload> innerPublisher,
      AssertSubscriber<Payload> innerAssertSubscriber) {
    rule.setRequestAcceptor(
        new AbstractRSocket() {
          @Override
          public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
            payloads.subscribe(innerAssertSubscriber);
            return innerPublisher.flux();
          }
        });

    rule.crs.requestChannel(outerPublisher).subscribe(outerAssertSubscriber);

    outerPublisher.assertWasSubscribed();
    outerAssertSubscriber.assertSubscribed();

    innerAssertSubscriber.assertNotSubscribed();
    innerPublisher.assertWasNotSubscribed();

    // firstRequest
    outerAssertSubscriber.request(1);
    outerPublisher.assertMaxRequested(1);
    outerPublisher.next(DefaultPayload.create("initialData", "initialMetadata"));

    innerAssertSubscriber.assertSubscribed();
    innerPublisher.assertWasSubscribed();
  }

  void nextFromOuterPublisher(
      TestPublisher<Payload> outerPublisher, AssertSubscriber<Payload> innerAssertSubscriber) {
    // ensures that outerUpstream and innerSubscriber is not terminated so the requestChannel
    outerPublisher.assertSubscribers(1);
    innerAssertSubscriber.assertNotTerminated();

    innerAssertSubscriber.request(6);
    outerPublisher.next(
        DefaultPayload.create("d1", "m1"),
        DefaultPayload.create("d2"),
        DefaultPayload.create("d3", "m3"),
        DefaultPayload.create("d4"),
        DefaultPayload.create("d5", "m5"));

    List<Payload> innerPayloads = innerAssertSubscriber.awaitAndAssertNextValueCount(6).values();
    Assertions.assertThat(innerPayloads.stream().map(Payload::getDataUtf8))
        .containsExactly("initialData", "d1", "d2", "d3", "d4", "d5");
    // fixme: incorrect behaviour of metadata encoding
    //    Assertions
    //            .assertThat(innerPayloads
    //                    .stream()
    //                    .map(Payload::hasMetadata)
    //            )
    //            .containsExactly(true, true, false, true, false, true);
    Assertions.assertThat(innerPayloads.stream().map(Payload::getMetadataUtf8))
        .containsExactly("initialMetadata", "m1", "", "m3", "", "m5");
  }

  void completeFromOuterPublisher(
      TestPublisher<Payload> outerPublisher, AssertSubscriber<Payload> innerAssertSubscriber) {
    // ensures that after sending complete upstream part is closed
    outerPublisher.complete();
    innerAssertSubscriber.assertTerminated();
    outerPublisher.assertNoSubscribers();
  }

  void cancelFromInnerSubscriber(
      TestPublisher<Payload> outerPublisher, AssertSubscriber<Payload> innerAssertSubscriber) {
    // ensures that after sending complete upstream part is closed
    innerAssertSubscriber.cancel();
    outerPublisher.assertWasCancelled();
    outerPublisher.assertNoSubscribers();
  }

  void nextFromInnerPublisher(
      TestPublisher<Payload> innerPublisher, AssertSubscriber<Payload> outerAssertSubscriber) {
    // ensures that downstream is not terminated so the requestChannel state is half-closed
    innerPublisher.assertSubscribers(1);
    outerAssertSubscriber.assertNotTerminated();

    // ensures innerPublisher can send messages and outerSubscriber can receive them
    outerAssertSubscriber.request(5);
    innerPublisher.next(
        DefaultPayload.create("rd1", "rm1"),
        DefaultPayload.create("rd2"),
        DefaultPayload.create("rd3", "rm3"),
        DefaultPayload.create("rd4"),
        DefaultPayload.create("rd5", "rm5"));

    List<Payload> outerPayloads = outerAssertSubscriber.awaitAndAssertNextValueCount(5).values();
    Assertions.assertThat(outerPayloads.stream().map(Payload::getDataUtf8))
        .containsExactly("rd1", "rd2", "rd3", "rd4", "rd5");
    // fixme: incorrect behaviour of metadata encoding
    //    Assertions
    //            .assertThat(outerPayloads
    //                    .stream()
    //                    .map(Payload::hasMetadata)
    //            )
    //            .containsExactly(true, false, true, false, true);
    Assertions.assertThat(outerPayloads.stream().map(Payload::getMetadataUtf8))
        .containsExactly("rm1", "", "rm3", "", "rm5");
  }

  void completeFromInnerPublisher(
      TestPublisher<Payload> innerPublisher, AssertSubscriber<Payload> outerAssertSubscriber) {
    // ensures that after sending complete inner upstream is closed
    innerPublisher.complete();
    outerAssertSubscriber.assertTerminated();
    innerPublisher.assertNoSubscribers();
  }

  void cancelFromOuterSubscriber(
      TestPublisher<Payload> outerPublisher,
      AssertSubscriber<Payload> outerAssertSubscriber,
      TestPublisher<Payload> innerPublisher,
      AssertSubscriber<Payload> innerAssertSubscriber) {
    // ensures that after sending cancel the whole requestChannel is terminated
    outerAssertSubscriber.cancel();
    innerPublisher.assertWasCancelled();
    innerPublisher.assertNoSubscribers();
    // ensures that cancellation is propagated to the actual upstream
    outerPublisher.assertWasCancelled();
    outerPublisher.assertNoSubscribers();
  }

  public static class SocketRule extends ExternalResource {

    DirectProcessor<ByteBuf> serverProcessor;
    DirectProcessor<ByteBuf> clientProcessor;
    private RSocketRequester crs;

    @SuppressWarnings("unused")
    private RSocketResponder srs;

    private RSocket requestAcceptor;
    private ArrayList<Throwable> clientErrors = new ArrayList<>();
    private ArrayList<Throwable> serverErrors = new ArrayList<>();

    @Override
    public Statement apply(Statement base, Description description) {
      return new Statement() {
        @Override
        public void evaluate() throws Throwable {
          init();
          base.evaluate();
        }
      };
    }

    protected void init() {
      serverProcessor = DirectProcessor.create();
      clientProcessor = DirectProcessor.create();

      LocalDuplexConnection serverConnection =
          new LocalDuplexConnection("server", clientProcessor, serverProcessor);
      LocalDuplexConnection clientConnection =
          new LocalDuplexConnection("client", serverProcessor, clientProcessor);

      requestAcceptor =
          null != requestAcceptor
              ? requestAcceptor
              : new AbstractRSocket() {
                @Override
                public Mono<Payload> requestResponse(Payload payload) {
                  return Mono.just(payload);
                }

                @Override
                public Flux<Payload> requestStream(Payload payload) {
                  return Flux.range(1, 10)
                      .map(
                          i -> DefaultPayload.create("server got -> [" + payload.toString() + "]"));
                }

                @Override
                public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
                  Flux.from(payloads)
                      .map(
                          payload ->
                              DefaultPayload.create("server got -> [" + payload.toString() + "]"))
                      .subscribe();

                  return Flux.range(1, 10)
                      .map(
                          payload ->
                              DefaultPayload.create("server got -> [" + payload.toString() + "]"));
                }
              };

      srs =
          new RSocketResponder(
              ByteBufAllocator.DEFAULT,
              serverConnection,
              requestAcceptor,
              PayloadDecoder.DEFAULT,
              throwable -> serverErrors.add(throwable),
              ResponderLeaseHandler.None,
              0);

      crs =
          new RSocketRequester(
              ByteBufAllocator.DEFAULT,
              clientConnection,
              PayloadDecoder.DEFAULT,
              throwable -> clientErrors.add(throwable),
              StreamIdSupplier.clientSupplier(),
              0,
              0,
              0,
              null,
              RequesterLeaseHandler.None);
    }

    public void setRequestAcceptor(RSocket requestAcceptor) {
      this.requestAcceptor = requestAcceptor;
      init();
    }

    public void assertNoErrors() {
      assertNoClientErrors();
      assertNoServerErrors();
    }

    public void assertNoClientErrors() {
      MatcherAssert.assertThat(
          "Unexpected error on the client connection.", clientErrors, is(empty()));
    }

    public void assertNoServerErrors() {
      MatcherAssert.assertThat(
          "Unexpected error on the server connection.", serverErrors, is(empty()));
    }

    public void assertClientError(String s) {
      assertError(s, "client", this.clientErrors);
    }

    public void assertServerError(String s) {
      assertError(s, "server", this.serverErrors);
    }
  }
}
