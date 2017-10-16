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

import static io.rsocket.FrameType.CANCEL;
import static io.rsocket.FrameType.KEEPALIVE;
import static io.rsocket.FrameType.NEXT_COMPLETE;
import static io.rsocket.FrameType.REQUEST_RESPONSE;
import static io.rsocket.FrameType.REQUEST_STREAM;
import static io.rsocket.test.util.TestSubscriber.anyPayload;
import static java.time.Duration.ofSeconds;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

import io.rsocket.exceptions.ApplicationException;
import io.rsocket.exceptions.RejectedSetupException;
import io.rsocket.extension.ParameterExtension;
import io.rsocket.frame.RequestFrameFlyweight;
import io.rsocket.test.util.TestSubscriber;
import io.rsocket.util.PayloadImpl;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@ExtendWith(ParameterExtension.class)
public class RSocketClientTest {
  private static final Duration TIMEOUT = ofSeconds(2);

  @Test
  public void testKeepAlive(ClientResource client) {
    FrameType type = assertTimeout(TIMEOUT, () -> client.connection.awaitSend().getType());

    assertThat("Unexpected frame sent.", type, is(KEEPALIVE));
  }

  @Test
  public void testInvalidFrameOnStream0(ClientResource client) {
    assertTimeout(TIMEOUT, () -> client.connection.addToReceivedBuffer(Frame.RequestN.from(0, 10)));

    assertThat("Unexpected errors.", client.errors, hasSize(1));
    assertThat(
        "Unexpected error received.",
        client.errors,
        contains(instanceOf(IllegalStateException.class)));
  }

  @Test
  public void testStreamInitialN(ClientResource client) {
    Frame frame =
        assertTimeout(
            TIMEOUT,
            () -> {
              Flux<Payload> stream = client.socket.requestStream(PayloadImpl.EMPTY);

              BaseSubscriber<Payload> subscriber =
                  new BaseSubscriber<Payload>() {
                    @Override
                    protected void hookOnSubscribe(Subscription subscription) {
                      // don't request here
                      //        subscription.request(3);
                    }
                  };
              stream.subscribe(subscriber);

              subscriber.request(5);

              List<Frame> sent =
                  client
                      .connection
                      .getSent()
                      .stream()
                      .filter(f -> f.getType() != KEEPALIVE)
                      .collect(Collectors.toList());

              assertThat("sent frame count", sent.size(), is(1));

              return sent.get(0);
            });

    assertThat("initial frame", frame.getType(), is(REQUEST_STREAM));
    assertThat("initial request n", RequestFrameFlyweight.initialRequestN(frame.content()), is(5));
  }

  @Test
  public void testHandleSetupException(ClientResource client) {
    assertTimeout(
        TIMEOUT,
        () ->
            client.connection.addToReceivedBuffer(
                Frame.Error.from(0, new RejectedSetupException("boom"))));

    assertThat("Unexpected errors.", client.errors, hasSize(1));
    assertThat(
        "Unexpected error received.",
        client.errors,
        contains(instanceOf(RejectedSetupException.class)));
  }

  @Test
  public void testHandleApplicationException(ClientResource client) {
    Subscriber<Payload> subscriber =
        assertTimeout(
            TIMEOUT,
            () -> {
              client.connection.clearSendReceiveBuffers();
              Publisher<Payload> response = client.socket.requestResponse(PayloadImpl.EMPTY);
              Subscriber<Payload> responseSub = TestSubscriber.create();
              response.subscribe(responseSub);

              int streamId = client.getStreamIdForRequestType(REQUEST_RESPONSE);
              client.connection.addToReceivedBuffer(
                  Frame.Error.from(streamId, new ApplicationException("error")));
              return responseSub;
            });

    verify(subscriber).onError(any(ApplicationException.class));
  }

  @Test
  public void testHandleValidFrame(ClientResource client) {
    Subscriber<Payload> subscriber =
        assertTimeout(
            TIMEOUT,
            () -> {
              Publisher<Payload> response = client.socket.requestResponse(PayloadImpl.EMPTY);
              Subscriber<Payload> sub = TestSubscriber.create();
              response.subscribe(sub);

              int streamId = client.getStreamIdForRequestType(REQUEST_RESPONSE);
              client.connection.addToReceivedBuffer(
                  Frame.PayloadFrame.from(streamId, NEXT_COMPLETE, PayloadImpl.EMPTY));
              return sub;
            });

    verify(subscriber).onNext(anyPayload());
    verify(subscriber).onComplete();
  }

  @Test
  public void testRequestReplyWithCancel(ClientResource client) {
    List<Frame> sent =
        assertTimeout(
            TIMEOUT,
            () -> {
              Mono<Payload> response = client.socket.requestResponse(PayloadImpl.EMPTY);

              try {
                response.block(Duration.ofMillis(100));
              } catch (IllegalStateException ise) {
              }

              return client
                  .connection
                  .getSent()
                  .stream()
                  .filter(f -> f.getType() != KEEPALIVE)
                  .collect(Collectors.toList());
            });

    assertThat(
        "Unexpected frame sent on the connection.", sent.get(0).getType(), is(REQUEST_RESPONSE));
    assertThat("Unexpected frame sent on the connection.", sent.get(1).getType(), is(CANCEL));
  }

  @Disabled
  @Test
  public void testRequestReplyErrorOnSend(ClientResource client) {
    Subscriber<Payload> subscriber =
        assertTimeout(
            TIMEOUT,
            () -> {
              client.connection.setAvailability(0); // Fails send
              Mono<Payload> response = client.socket.requestResponse(PayloadImpl.EMPTY);
              Subscriber<Payload> responseSub = TestSubscriber.create();
              response.subscribe(responseSub);
              return responseSub;
            });

    verify(subscriber).onError(any(RuntimeException.class));
  }

  @Test
  public void testLazyRequestResponse(ClientResource client) {
    Publisher<Payload> response = client.socket.requestResponse(PayloadImpl.EMPTY);
    int streamId = assertTimeout(TIMEOUT, () -> client.sendRequestResponse(response));
    client.connection.clearSendReceiveBuffers();
    int streamId2 = assertTimeout(TIMEOUT, () -> client.sendRequestResponse(response));
    assertThat("Stream ID reused.", streamId2, not(equalTo(streamId)));
  }
}
