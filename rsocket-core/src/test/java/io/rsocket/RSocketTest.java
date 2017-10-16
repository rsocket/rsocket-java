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

import static java.time.Duration.ofSeconds;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

import io.rsocket.exceptions.ApplicationException;
import io.rsocket.extension.ParameterExtension;
import io.rsocket.test.util.TestSubscriber;
import io.rsocket.util.PayloadImpl;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@ExtendWith(ParameterExtension.class)
public class RSocketTest {
  private static final Duration TIMEOUT = ofSeconds(2);

  @Test
  public void testRequestReplyNoError(SocketResource socket) {
    Subscriber<Payload> subscriber =
        assertTimeout(
            TIMEOUT,
            () -> {
              Subscriber<Payload> s = TestSubscriber.create();
              socket.crs.requestResponse(new PayloadImpl("hello")).subscribe(s);
              return s;
            });
    verify(subscriber).onNext(TestSubscriber.anyPayload());
    verify(subscriber).onComplete();
    assertNoErrors(socket);
  }

  @Disabled
  @Test
  public void testHandlerEmitsError(SocketResource socket) {
    Subscriber<Payload> subscriber =
        assertTimeout(
            TIMEOUT,
            () -> {
              socket.acceptor(
                  payload -> Mono.error(new NullPointerException("Deliberate exception.")));
              Subscriber<Payload> s = TestSubscriber.create();
              socket.crs.requestResponse(PayloadImpl.EMPTY).subscribe(s);
              return s;
            });
    verify(subscriber).onError(any(ApplicationException.class));
    assertNoErrors(socket);
  }

  @Test
  public void testChannel(SocketResource socket) {
    assertTimeout(
        TIMEOUT,
        () -> {
          CountDownLatch latch = new CountDownLatch(10);
          Flux<Payload> requests =
              Flux.range(0, 10).map(i -> new PayloadImpl("streaming in -> " + i));

          Flux<Payload> responses = socket.crs.requestChannel(requests);

          responses.doOnNext(p -> latch.countDown()).subscribe();

          latch.await();
        });
  }

  private void assertNoErrors(SocketResource socket) {
    assertThat("Unexpected error on the client connection.", socket.clientErrors, is(empty()));
    assertThat("Unexpected error on the server connection.", socket.serverErrors, is(empty()));
  }
}
