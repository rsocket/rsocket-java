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

package io.rsocket;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

import io.rsocket.exceptions.ApplicationException;
import io.rsocket.test.util.LocalDuplexConnection;
import io.rsocket.test.util.TestSubscriber;
import io.rsocket.util.PayloadImpl;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RSocketTest {

  @Rule public final SocketRule rule = new SocketRule();

  @Test(timeout = 2_000)
  public void testRequestReplyNoError() {
    Subscriber<Payload> subscriber = TestSubscriber.create();
    rule.crs.requestResponse(new PayloadImpl("hello")).subscribe(subscriber);
    verify(subscriber).onNext(TestSubscriber.anyPayload());
    verify(subscriber).onComplete();
    rule.assertNoErrors();
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
    rule.crs.requestResponse(PayloadImpl.EMPTY).subscribe(subscriber);
    verify(subscriber).onError(any(ApplicationException.class));

    // Client sees error through normal API
    rule.assertNoClientErrors();

    rule.assertServerError("java.lang.NullPointerException: Deliberate exception.");
  }

  @Test(timeout = 2000)
  public void testChannel() throws Exception {
    CountDownLatch latch = new CountDownLatch(10);
    Flux<Payload> requests = Flux.range(0, 10).map(i -> new PayloadImpl("streaming in -> " + i));

    Flux<Payload> responses = rule.crs.requestChannel(requests);

    responses.doOnNext(p -> latch.countDown()).subscribe();

    latch.await();
  }

  public static class SocketRule extends ExternalResource {

    private RSocketClient crs;
    private RSocketServer srs;
    private RSocket requestAcceptor;
    DirectProcessor<Frame> serverProcessor;
    DirectProcessor<Frame> clientProcessor;
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
                public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
                  Flux.from(payloads)
                      .map(payload -> new PayloadImpl("server got -> [" + payload.toString() + "]"))
                      .subscribe();

                  return Flux.range(1, 10)
                      .map(
                          payload -> new PayloadImpl("server got -> [" + payload.toString() + "]"));
                }
              };

      srs =
          new RSocketServer(
              serverConnection, requestAcceptor, throwable -> serverErrors.add(throwable));

      crs =
          new RSocketClient(
              clientConnection,
              throwable -> clientErrors.add(throwable),
              StreamIdSupplier.clientSupplier());
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

  public static void assertError(String s, String mode, ArrayList<Throwable> errors) {
    for (Throwable t : errors) {
      if (t.toString().equals(s)) {
        return;
      }
    }

    Assert.fail("Expected " + mode + " connection error: " + s + " other errors " + errors.size());
  }
}
