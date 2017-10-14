/*
 * Copyright 2016 Netflix, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.rsocket.frame;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.rsocket.Frame;
import io.rsocket.FrameType;
import io.rsocket.util.PayloadImpl;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

public class RequestFrameFlyweightTest {
  private final ByteBuf byteBuf = Unpooled.buffer(1024);

  @Test
  public void testEncoding() {
    int encoded =
        RequestFrameFlyweight.encode(
            byteBuf,
            1,
            FrameHeaderFlyweight.FLAGS_M,
            FrameType.REQUEST_STREAM,
            1,
            Unpooled.copiedBuffer("md", StandardCharsets.UTF_8),
            Unpooled.copiedBuffer("d", StandardCharsets.UTF_8));
    assertEquals(
        "000010000000011900000000010000026d6464", ByteBufUtil.hexDump(byteBuf, 0, encoded));

    PayloadImpl payload =
        new PayloadImpl(Frame.from(stringToBuf("000010000000011900000000010000026d6464")));

    assertEquals("md", StandardCharsets.UTF_8.decode(payload.getMetadata()).toString());
  }

  @Test
  public void testEncodingWithEmptyMetadata() {
    int encoded =
        RequestFrameFlyweight.encode(
            byteBuf,
            1,
            FrameHeaderFlyweight.FLAGS_M,
            FrameType.REQUEST_STREAM,
            1,
            Unpooled.copiedBuffer("", StandardCharsets.UTF_8),
            Unpooled.copiedBuffer("d", StandardCharsets.UTF_8));
    assertEquals("00000e0000000119000000000100000064", ByteBufUtil.hexDump(byteBuf, 0, encoded));

    PayloadImpl payload =
        new PayloadImpl(Frame.from(stringToBuf("00000e0000000119000000000100000064")));

    assertEquals("", StandardCharsets.UTF_8.decode(payload.getMetadata()).toString());
  }

  @Test
  public void testEncodingWithNullMetadata() {
    int encoded =
        RequestFrameFlyweight.encode(
            byteBuf,
            1,
            0,
            FrameType.REQUEST_STREAM,
            1,
            null,
            Unpooled.copiedBuffer("d", StandardCharsets.UTF_8));
    assertEquals("00000b0000000118000000000164", ByteBufUtil.hexDump(byteBuf, 0, encoded));

    PayloadImpl payload = new PayloadImpl(Frame.from(stringToBuf("00000b0000000118000000000164")));

    assertFalse(payload.hasMetadata());
  }

  private String bufToString(int encoded) {
    return ByteBufUtil.hexDump(byteBuf, 0, encoded);
  }

  private ByteBuf stringToBuf(CharSequence s) {
    return Unpooled.wrappedBuffer(ByteBufUtil.decodeHexDump(s));
  }
}
