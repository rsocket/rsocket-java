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

import static io.rsocket.FrameType.*;
import static io.rsocket.test.util.TestSubscriber.anyPayload;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

import io.rsocket.exceptions.ApplicationException;
import io.rsocket.exceptions.RejectedSetupException;
import io.rsocket.frame.RequestFrameFlyweight;
import io.rsocket.test.util.TestSubscriber;
import io.rsocket.util.PayloadImpl;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Rule;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RSocketClientTest {

  @Rule public final ClientSocketRule rule = new ClientSocketRule();

  @Test(timeout = 2_000)
  public void testKeepAlive() throws Exception {
    assertThat("Unexpected frame sent.", rule.connection.awaitSend().getType(), is(KEEPALIVE));
  }

  @Test(timeout = 2_000)
  public void testInvalidFrameOnStream0() {
    rule.connection.addToReceivedBuffer(Frame.RequestN.from(0, 10));
    assertThat("Unexpected errors.", rule.errors, hasSize(1));
    assertThat(
        "Unexpected error received.",
        rule.errors,
        contains(instanceOf(IllegalStateException.class)));
  }

  @Test(timeout = 2_000)
  public void testStreamInitialN() {
    Flux<Payload> stream = rule.socket.requestStream(PayloadImpl.EMPTY);

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
        rule.connection
            .getSent()
            .stream()
            .filter(f -> f.getType() != KEEPALIVE)
            .collect(Collectors.toList());

    assertThat("sent frame count", sent.size(), is(1));

    Frame f = sent.get(0);

    assertThat("initial frame", f.getType(), is(REQUEST_STREAM));
    assertThat("initial request n", RequestFrameFlyweight.initialRequestN(f.content()), is(5));
  }

  @Test(timeout = 2_000)
  public void testHandleSetupException() {
    rule.connection.addToReceivedBuffer(Frame.Error.from(0, new RejectedSetupException("boom")));
    assertThat("Unexpected errors.", rule.errors, hasSize(1));
    assertThat(
        "Unexpected error received.",
        rule.errors,
        contains(instanceOf(RejectedSetupException.class)));
  }

  @Test(timeout = 2_000)
  public void testHandleApplicationException() {
    rule.connection.clearSendReceiveBuffers();
    Publisher<Payload> response = rule.socket.requestResponse(PayloadImpl.EMPTY);
    Subscriber<Payload> responseSub = TestSubscriber.create();
    response.subscribe(responseSub);

    int streamId = rule.getStreamIdForRequestType(REQUEST_RESPONSE);
    rule.connection.addToReceivedBuffer(
        Frame.Error.from(streamId, new ApplicationException("error")));

    verify(responseSub).onError(any(ApplicationException.class));
  }

  @Test(timeout = 2_000)
  public void testHandleValidFrame() {
    Publisher<Payload> response = rule.socket.requestResponse(PayloadImpl.EMPTY);
    Subscriber<Payload> sub = TestSubscriber.create();
    response.subscribe(sub);

    int streamId = rule.getStreamIdForRequestType(REQUEST_RESPONSE);
    rule.connection.addToReceivedBuffer(
        Frame.PayloadFrame.from(streamId, NEXT_COMPLETE, PayloadImpl.EMPTY));

    verify(sub).onNext(anyPayload());
    verify(sub).onComplete();
  }

  @Test(timeout = 2_000)
  public void testRequestReplyWithCancel() {
    Mono<Payload> response = rule.socket.requestResponse(PayloadImpl.EMPTY);

    try {
      response.block(Duration.ofMillis(100));
    } catch (IllegalStateException ise) {
    }

    List<Frame> sent =
        rule.connection
            .getSent()
            .stream()
            .filter(f -> f.getType() != KEEPALIVE)
            .collect(Collectors.toList());

    assertThat(
        "Unexpected frame sent on the connection.", sent.get(0).getType(), is(REQUEST_RESPONSE));
    assertThat("Unexpected frame sent on the connection.", sent.get(1).getType(), is(CANCEL));
  }

  @Test(timeout = 2_000)
  public void testRequestReplyErrorOnSend() {
    rule.connection.setAvailability(0); // Fails send
    Mono<Payload> response = rule.socket.requestResponse(PayloadImpl.EMPTY);
    Subscriber<Payload> responseSub = TestSubscriber.create(10);
    response.subscribe(responseSub);

    this.rule.assertNoConnectionErrors();

    verify(responseSub).onSubscribe(any(Subscription.class));

    // TODO this should get the error reported through the response subscription
    //    verify(responseSub).onError(any(RuntimeException.class));
  }

  @Test(timeout = 2_000)
  public void testLazyRequestResponse() {
    Publisher<Payload> response = rule.socket.requestResponse(PayloadImpl.EMPTY);
    int streamId = sendRequestResponse(response);
    rule.connection.clearSendReceiveBuffers();
    int streamId2 = sendRequestResponse(response);
    assertThat("Stream ID reused.", streamId2, not(equalTo(streamId)));
  }

  public int sendRequestResponse(Publisher<Payload> response) {
    Subscriber<Payload> sub = TestSubscriber.create();
    response.subscribe(sub);
    int streamId = rule.getStreamIdForRequestType(REQUEST_RESPONSE);
    rule.connection.addToReceivedBuffer(
        Frame.PayloadFrame.from(streamId, NEXT_COMPLETE, PayloadImpl.EMPTY));
    verify(sub).onNext(anyPayload());
    verify(sub).onComplete();
    return streamId;
  }

  public static class ClientSocketRule extends AbstractSocketRule<RSocketClient> {
    @Override
    protected RSocketClient newRSocket() {
      return new RSocketClient(
          connection,
          throwable -> errors.add(throwable),
          StreamIdSupplier.clientSupplier(),
          Duration.ofMillis(100),
          Duration.ofMillis(100),
          4);
    }

    public int getStreamIdForRequestType(FrameType expectedFrameType) {
      assertThat("Unexpected frames sent.", connection.getSent(), hasSize(greaterThanOrEqualTo(1)));
      List<FrameType> framesFound = new ArrayList<>();
      for (Frame frame : connection.getSent()) {
        if (frame.getType() == expectedFrameType) {
          return frame.getStreamId();
        }
        framesFound.add(frame.getType());
      }
      throw new AssertionError(
          "No frames sent with frame type: "
              + expectedFrameType
              + ", frames found: "
              + framesFound);
    }
  }
}
