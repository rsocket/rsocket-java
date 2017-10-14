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
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTimeout;

import io.netty.buffer.Unpooled;
import io.rsocket.extension.ParameterExtension;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Mono;

@ExtendWith(ParameterExtension.class)
public class RSocketServerTest {
  private static final Duration TIMEOUT = ofSeconds(2);

  @Disabled
  @Test
  public void testHandleKeepAlive(ServerResource server) {
    Frame sent =
        assertTimeout(
            TIMEOUT,
            () -> {
              server.connection.addToReceivedBuffer(
                  Frame.Keepalive.from(Unpooled.EMPTY_BUFFER, true));
              return server.connection.awaitSend();
            });
    assertThat("Unexpected frame sent.", sent.getType(), is(FrameType.KEEPALIVE));
    /*Keep alive ack must not have respond flag else, it will result in infinite ping-pong of keep alive frames.*/
    assertThat(
        "Unexpected keep-alive frame respond flag.",
        Frame.Keepalive.hasRespondFlag(sent),
        is(false));
  }

  @Disabled
  @Test
  public void testHandleResponseFrameNoError(ServerResource server) throws Exception {
    final int streamId = 4;
    Collection<Subscriber<Frame>> sendSubscribers =
        assertTimeout(
            TIMEOUT,
            () -> {
              server.connection.clearSendReceiveBuffers();

              server.sendRequest(streamId, FrameType.REQUEST_RESPONSE);

              return server.connection.getSendSubscribers();
            });
    assertThat("Request not sent.", sendSubscribers, hasSize(1));
    assertThat("Unexpected error.", server.errors, is(empty()));
    Subscriber<Frame> sendSub = sendSubscribers.iterator().next();
    assertThat(
        "Unexpected frame sent.",
        server.connection.awaitSend().getType(),
        anyOf(is(FrameType.COMPLETE), is(FrameType.NEXT_COMPLETE)));
  }

  @Test
  public void testHandlerEmitsError(ServerResource server) throws Exception {
    final int streamId = 4;
    assertTimeout(TIMEOUT, () -> server.sendRequest(streamId, FrameType.REQUEST_STREAM));
    assertThat("Unexpected error.", server.errors, is(empty()));
    assertThat(
        "Unexpected frame sent.", server.connection.awaitSend().getType(), is(FrameType.ERROR));
  }

  @Test
  public void testCancel(ServerResource server) {
    final int streamId = 4;
    final AtomicBoolean cancelled = new AtomicBoolean();
    assertTimeout(
        TIMEOUT,
        () -> {
          server.acceptor(payload -> Mono.<Payload>never().doOnCancel(() -> cancelled.set(true)));
          server.sendRequest(streamId, FrameType.REQUEST_RESPONSE);
        });

    assertThat("Unexpected error.", server.errors, is(empty()));
    assertThat("Unexpected frame sent.", server.connection.getSent(), is(empty()));

    server.connection.addToReceivedBuffer(Frame.Cancel.from(streamId));
    assertThat("Unexpected frame sent.", server.connection.getSent(), is(empty()));
    assertThat("Subscription not cancelled.", cancelled.get(), is(true));
  }
}
