/*
 * Copyright 2015-2018 the original author or authors.
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

package io.rsocket;

import static io.rsocket.frame.FrameHeaderFlyweight.frameType;
import static io.rsocket.frame.FrameType.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.exceptions.ApplicationErrorException;
import io.rsocket.exceptions.RejectedSetupException;
import io.rsocket.frame.*;
import io.rsocket.test.util.TestSubscriber;
import io.rsocket.util.DefaultPayload;
import io.rsocket.util.EmptyPayload;
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
import reactor.core.publisher.MonoProcessor;

public class RSocketClientTest {

  @Rule public final ClientSocketRule rule = new ClientSocketRule();

  @Test(timeout = 2_000)
  public void testInvalidFrameOnStream0() {
    rule.connection.addToReceivedBuffer(
        RequestNFrameFlyweight.encode(ByteBufAllocator.DEFAULT, 0, 10));
    assertThat("Unexpected errors.", rule.errors, hasSize(1));
    assertThat(
        "Unexpected error received.",
        rule.errors,
        contains(instanceOf(IllegalStateException.class)));
  }

  @Test(timeout = 2_000)
  public void testStreamInitialN() {
    Flux<Payload> stream = rule.socket.requestStream(EmptyPayload.INSTANCE);

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

    List<ByteBuf> sent =
        rule.connection
            .getSent()
            .stream()
            .filter(f -> frameType(f) != KEEPALIVE)
            .collect(Collectors.toList());

    assertThat("sent frame count", sent.size(), is(1));

    ByteBuf f = sent.get(0);

    assertThat("initial frame", frameType(f), is(REQUEST_STREAM));
    assertThat("initial request n", RequestStreamFrameFlyweight.initialRequestN(f), is(5));
  }

  @Test(timeout = 2_000)
  public void testHandleSetupException() {
    rule.connection.addToReceivedBuffer(
        ErrorFrameFlyweight.encode(
            ByteBufAllocator.DEFAULT, 0, new RejectedSetupException("boom")));
    assertThat("Unexpected errors.", rule.errors, hasSize(1));
    assertThat(
        "Unexpected error received.",
        rule.errors,
        contains(instanceOf(RejectedSetupException.class)));
  }

  @Test(timeout = 2_000)
  public void testHandleApplicationException() {
    rule.connection.clearSendReceiveBuffers();
    Publisher<Payload> response = rule.socket.requestResponse(EmptyPayload.INSTANCE);
    Subscriber<Payload> responseSub = TestSubscriber.create();
    response.subscribe(responseSub);

    int streamId = rule.getStreamIdForRequestType(REQUEST_RESPONSE);
    rule.connection.addToReceivedBuffer(
        ErrorFrameFlyweight.encode(
            ByteBufAllocator.DEFAULT, streamId, new ApplicationErrorException("error")));

    verify(responseSub).onError(any(ApplicationErrorException.class));
  }

  @Test(timeout = 2_000)
  public void testHandleValidFrame() {
    Publisher<Payload> response = rule.socket.requestResponse(EmptyPayload.INSTANCE);
    Subscriber<Payload> sub = TestSubscriber.create();
    response.subscribe(sub);

    int streamId = rule.getStreamIdForRequestType(REQUEST_RESPONSE);
    rule.connection.addToReceivedBuffer(
        PayloadFrameFlyweight.encodeNext(
            ByteBufAllocator.DEFAULT, streamId, EmptyPayload.INSTANCE));

    verify(sub).onComplete();
  }

  @Test(timeout = 2_000)
  public void testRequestReplyWithCancel() {
    Mono<Payload> response = rule.socket.requestResponse(EmptyPayload.INSTANCE);

    try {
      response.block(Duration.ofMillis(100));
    } catch (IllegalStateException ise) {
    }

    List<ByteBuf> sent =
        rule.connection
            .getSent()
            .stream()
            .filter(f -> frameType(f) != KEEPALIVE)
            .collect(Collectors.toList());

    assertThat(
        "Unexpected frame sent on the connection.", frameType(sent.get(0)), is(REQUEST_RESPONSE));
    assertThat("Unexpected frame sent on the connection.", frameType(sent.get(1)), is(CANCEL));
  }

  @Test(timeout = 2_000)
  public void testRequestReplyErrorOnSend() {
    rule.connection.setAvailability(0); // Fails send
    Mono<Payload> response = rule.socket.requestResponse(EmptyPayload.INSTANCE);
    Subscriber<Payload> responseSub = TestSubscriber.create(10);
    response.subscribe(responseSub);

    this.rule.assertNoConnectionErrors();

    verify(responseSub).onSubscribe(any(Subscription.class));

    // TODO this should get the error reported through the response subscription
    //    verify(responseSub).onError(any(RuntimeException.class));
  }

  @Test(timeout = 2_000)
  public void testLazyRequestResponse() {
    Publisher<Payload> response = rule.socket.requestResponse(EmptyPayload.INSTANCE);
    int streamId = sendRequestResponse(response);
    rule.connection.clearSendReceiveBuffers();
    int streamId2 = sendRequestResponse(response);
    assertThat("Stream ID reused.", streamId2, not(equalTo(streamId)));
  }

  @Test
  public void testChannelRequestCancellation() {
    MonoProcessor<Void> cancelled = MonoProcessor.create();
    Flux<Payload> request = Flux.<Payload>never().doOnCancel(cancelled::onComplete);
    rule.socket.requestChannel(request).subscribe().dispose();
    Flux.first(
            cancelled,
            Flux.error(new IllegalStateException("Channel request not cancelled"))
                .delaySubscription(Duration.ofSeconds(1)))
        .blockFirst();
  }

  public int sendRequestResponse(Publisher<Payload> response) {
    Subscriber<Payload> sub = TestSubscriber.create();
    response.subscribe(sub);
    int streamId = rule.getStreamIdForRequestType(REQUEST_RESPONSE);
    rule.connection.addToReceivedBuffer(
        PayloadFrameFlyweight.encodeNextComplete(
            ByteBufAllocator.DEFAULT, streamId, EmptyPayload.INSTANCE));
    verify(sub).onNext(any(Payload.class));
    verify(sub).onComplete();
    return streamId;
  }

  public static class ClientSocketRule extends AbstractSocketRule<RSocketClient> {
    @Override
    protected RSocketClient newRSocket() {
      return new RSocketClient(
          ByteBufAllocator.DEFAULT,
          connection,
          DefaultPayload::create,
          throwable -> errors.add(throwable),
          StreamIdSupplier.clientSupplier());
    }

    public int getStreamIdForRequestType(FrameType expectedFrameType) {
      assertThat("Unexpected frames sent.", connection.getSent(), hasSize(greaterThanOrEqualTo(1)));
      List<FrameType> framesFound = new ArrayList<>();
      for (ByteBuf frame : connection.getSent()) {
        FrameType frameType = frameType(frame);
        if (frameType == expectedFrameType) {
          return FrameHeaderFlyweight.streamId(frame);
        }
        framesFound.add(frameType);
      }
      throw new AssertionError(
          "No frames sent with frame type: "
              + expectedFrameType
              + ", frames found: "
              + framesFound);
    }
  }
}
