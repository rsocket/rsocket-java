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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import io.netty.buffer.Unpooled;
import io.rsocket.test.util.TestDuplexConnection;
import io.rsocket.test.util.TestSubscriber;
import io.rsocket.util.PayloadImpl;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Mono;

public class RSocketServerTest {

  @Rule public final ServerSocketRule rule = new ServerSocketRule();

  @Test(timeout = 2000)
  @Ignore
  public void testHandleKeepAlive() throws Exception {
    rule.connection.addToReceivedBuffer(Frame.Keepalive.from(Unpooled.EMPTY_BUFFER, true));
    Frame sent = rule.connection.awaitSend();
    assertThat("Unexpected frame sent.", sent.getType(), is(FrameType.KEEPALIVE));
    /*Keep alive ack must not have respond flag else, it will result in infinite ping-pong of keep alive frames.*/
    assertThat(
        "Unexpected keep-alive frame respond flag.",
        Frame.Keepalive.hasRespondFlag(sent),
        is(false));
  }

  @Test(timeout = 2000)
  @Ignore
  public void testHandleResponseFrameNoError() throws Exception {
    final int streamId = 4;
    rule.connection.clearSendReceiveBuffers();

    rule.sendRequest(streamId, FrameType.REQUEST_RESPONSE);

    Collection<Subscriber<Frame>> sendSubscribers = rule.connection.getSendSubscribers();
    assertThat("Request not sent.", sendSubscribers, hasSize(1));
    assertThat("Unexpected error.", rule.errors, is(empty()));
    Subscriber<Frame> sendSub = sendSubscribers.iterator().next();
    assertThat(
        "Unexpected frame sent.",
        rule.connection.awaitSend().getType(),
        anyOf(is(FrameType.COMPLETE), is(FrameType.NEXT_COMPLETE)));
  }

  @Test(timeout = 2000)
  @Ignore
  public void testHandlerEmitsError() throws Exception {
    final int streamId = 4;
    rule.sendRequest(streamId, FrameType.REQUEST_STREAM);
    assertThat("Unexpected error.", rule.errors, is(empty()));
    assertThat(
        "Unexpected frame sent.", rule.connection.awaitSend().getType(), is(FrameType.ERROR));
  }

  @Test(timeout = 2_0000)
  public void testCancel() {
    final int streamId = 4;
    final AtomicBoolean cancelled = new AtomicBoolean();
    rule.setAcceptingSocket(
        new AbstractRSocket() {
          @Override
          public Mono<Payload> requestResponse(Payload payload) {
            return Mono.<Payload>never().doOnCancel(() -> cancelled.set(true));
          }
        });
    rule.sendRequest(streamId, FrameType.REQUEST_RESPONSE);

    assertThat("Unexpected error.", rule.errors, is(empty()));
    assertThat("Unexpected frame sent.", rule.connection.getSent(), is(empty()));

    rule.connection.addToReceivedBuffer(Frame.Cancel.from(streamId));
    assertThat("Unexpected frame sent.", rule.connection.getSent(), is(empty()));
    assertThat("Subscription not cancelled.", cancelled.get(), is(true));
  }

  public static class ServerSocketRule extends AbstractSocketRule<RSocketServer> {

    private RSocket acceptingSocket;

    @Override
    protected void init() {
      acceptingSocket =
          new AbstractRSocket() {
            @Override
            public Mono<Payload> requestResponse(Payload payload) {
              return Mono.just(payload);
            }
          };
      super.init();
    }

    public void setAcceptingSocket(RSocket acceptingSocket) {
      this.acceptingSocket = acceptingSocket;
      connection = new TestDuplexConnection();
      connectSub = TestSubscriber.create();
      errors = new ConcurrentLinkedQueue<>();
      super.init();
    }

    @Override
    protected RSocketServer newRSocket() {
      return new RSocketServer(connection, acceptingSocket, throwable -> errors.add(throwable));
    }

    private void sendRequest(int streamId, FrameType frameType) {
      Frame request = Frame.Request.from(streamId, frameType, PayloadImpl.EMPTY, 1);
      connection.addToReceivedBuffer(request);
      connection.addToReceivedBuffer(Frame.RequestN.from(streamId, 2));
    }
  }
}
