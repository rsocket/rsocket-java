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

package io.rsocket.core;

import static io.rsocket.frame.FrameLengthCodec.FRAME_LENGTH_MASK;

import io.netty.buffer.ByteBufAllocator;
import io.rsocket.RSocket;
import io.rsocket.buffer.LeaksTrackingByteBufAllocator;
import io.rsocket.test.util.TestDuplexConnection;
import io.rsocket.test.util.TestSubscriber;
import java.time.Duration;
import org.reactivestreams.Subscriber;

public abstract class AbstractSocketRule<T extends RSocket> {

  protected TestDuplexConnection connection;
  protected Subscriber<Void> connectSub;
  protected T socket;
  protected LeaksTrackingByteBufAllocator allocator;
  protected int maxFrameLength = FRAME_LENGTH_MASK;
  protected int maxInboundPayloadSize = Integer.MAX_VALUE;

  public void init() {
    allocator =
        LeaksTrackingByteBufAllocator.instrument(
            ByteBufAllocator.DEFAULT, Duration.ofSeconds(5), "");
    connectSub = TestSubscriber.create();
    doInit();
  }

  protected void doInit() {
    if (connection != null) {
      connection.dispose();
    }
    if (socket != null) {
      socket.dispose();
    }
    connection = new TestDuplexConnection(allocator);
    socket = newRSocket();
  }

  public void setMaxInboundPayloadSize(int maxInboundPayloadSize) {
    this.maxInboundPayloadSize = maxInboundPayloadSize;
    doInit();
  }

  public void setMaxFrameLength(int maxFrameLength) {
    this.maxFrameLength = maxFrameLength;
    doInit();
  }

  protected abstract T newRSocket();

  public LeaksTrackingByteBufAllocator alloc() {
    return allocator;
  }

  public void assertHasNoLeaks() {
    allocator.assertHasNoLeaks();
  }
}
