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

package io.rsocket.framing;

import static io.netty.buffer.Unpooled.EMPTY_BUFFER;
import static io.netty.buffer.UnpooledByteBufAllocator.DEFAULT;
import static io.rsocket.framing.CancelFrame.createCancelFrame;
import static io.rsocket.framing.ErrorFrame.createErrorFrame;
import static io.rsocket.framing.ExtensionFrame.createExtensionFrame;
import static io.rsocket.framing.FrameLengthFrame.createFrameLengthFrame;
import static io.rsocket.framing.KeepaliveFrame.createKeepaliveFrame;
import static io.rsocket.framing.LeaseFrame.createLeaseFrame;
import static io.rsocket.framing.MetadataPushFrame.createMetadataPushFrame;
import static io.rsocket.framing.PayloadFrame.createPayloadFrame;
import static io.rsocket.framing.RequestChannelFrame.createRequestChannelFrame;
import static io.rsocket.framing.RequestFireAndForgetFrame.createRequestFireAndForgetFrame;
import static io.rsocket.framing.RequestNFrame.createRequestNFrame;
import static io.rsocket.framing.RequestResponseFrame.createRequestResponseFrame;
import static io.rsocket.framing.RequestStreamFrame.createRequestStreamFrame;
import static io.rsocket.framing.ResumeFrame.createResumeFrame;
import static io.rsocket.framing.ResumeOkFrame.createResumeOkFrame;
import static io.rsocket.framing.SetupFrame.createSetupFrame;
import static io.rsocket.framing.StreamIdFrame.createStreamIdFrame;

import io.netty.buffer.ByteBuf;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Function;

public final class TestFrames {

  private TestFrames() {}

  public static CancelFrame createTestCancelFrame() {
    return createCancelFrame(DEFAULT);
  }

  public static ErrorFrame createTestErrorFrame() {
    return createErrorFrame(DEFAULT, 1, (ByteBuf) null);
  }

  public static ExtensionFrame createTestExtensionFrame() {
    return createExtensionFrame(DEFAULT, true, 1, (ByteBuf) null, null);
  }

  public static Frame createTestFrame(ByteBuf byteBuf) {
    return new TestFrame(byteBuf);
  }

  public static FrameLengthFrame createTestFrameLengthFrame() {
    return createFrameLengthFrame(DEFAULT, createTestStreamIdFrame());
  }

  public static KeepaliveFrame createTestKeepaliveFrame() {
    return createKeepaliveFrame(DEFAULT, false, 1, null);
  }

  public static LeaseFrame createTestLeaseFrame() {
    return createLeaseFrame(DEFAULT, Duration.ofMillis(1), 1, null);
  }

  public static MetadataPushFrame createTestMetadataPushFrame() {
    return createMetadataPushFrame(DEFAULT, EMPTY_BUFFER);
  }

  public static PayloadFrame createTestPayloadFrame() {
    return createPayloadFrame(DEFAULT, false, true, (ByteBuf) null, null);
  }

  public static RequestChannelFrame createTestRequestChannelFrame() {
    return createRequestChannelFrame(DEFAULT, false, false, 1, (ByteBuf) null, null);
  }

  public static RequestFireAndForgetFrame createTestRequestFireAndForgetFrame() {
    return createRequestFireAndForgetFrame(DEFAULT, false, (ByteBuf) null, null);
  }

  public static RequestNFrame createTestRequestNFrame() {
    return createRequestNFrame(DEFAULT, 1);
  }

  public static RequestResponseFrame createTestRequestResponseFrame() {
    return createRequestResponseFrame(DEFAULT, false, (ByteBuf) null, null);
  }

  public static RequestStreamFrame createTestRequestStreamFrame() {
    return createRequestStreamFrame(DEFAULT, false, 1, (ByteBuf) null, null);
  }

  public static ResumeFrame createTestResumeFrame() {
    return createResumeFrame(DEFAULT, 1, 0, EMPTY_BUFFER, 1, 1);
  }

  public static ResumeOkFrame createTestResumeOkFrame() {
    return createResumeOkFrame(DEFAULT, 1);
  }

  public static SetupFrame createTestSetupFrame() {
    return createSetupFrame(
        DEFAULT, true, 1, 1, Duration.ofMillis(1), Duration.ofMillis(1), null, "", "", null, null);
  }

  public static StreamIdFrame createTestStreamIdFrame() {
    return createStreamIdFrame(DEFAULT, 1, createTestCancelFrame());
  }

  private static final class TestFrame implements Frame {

    private final ByteBuf byteBuf;

    private TestFrame(ByteBuf byteBuf) {
      this.byteBuf = byteBuf;
    }

    @Override
    public void consumeFrame(Consumer<ByteBuf> consumer) {
      consumer.accept(byteBuf.asReadOnly());
    }

    @Override
    public void dispose() {}

    @Override
    public <T> T mapFrame(Function<ByteBuf, T> function) {
      return function.apply(byteBuf.asReadOnly());
    }
  }
}
