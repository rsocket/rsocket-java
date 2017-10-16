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
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.rsocket.FrameType;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

public class SetupFrameFlyweightTest {
  private final ByteBuf byteBuf = Unpooled.buffer(1024);

  @Test
  public void validFrame() {
    ByteBuf metadata = Unpooled.wrappedBuffer(new byte[] {1, 2, 3, 4});
    ByteBuf data = Unpooled.wrappedBuffer(new byte[] {5, 4, 3});
    SetupFrameFlyweight.encode(byteBuf, 0, 5, 500, "metadata_type", "data_type", metadata, data);

    metadata.resetReaderIndex();
    data.resetReaderIndex();

    assertEquals(FrameType.SETUP, FrameHeaderFlyweight.frameType(byteBuf));
    assertEquals("metadata_type", SetupFrameFlyweight.metadataMimeType(byteBuf));
    assertEquals("data_type", SetupFrameFlyweight.dataMimeType(byteBuf));
    assertEquals(metadata, FrameHeaderFlyweight.sliceFrameMetadata(byteBuf));
    assertEquals(data, FrameHeaderFlyweight.sliceFrameData(byteBuf));
  }

  @Test
  public void resumeNotSupported() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            SetupFrameFlyweight.encode(
                byteBuf,
                SetupFrameFlyweight.FLAGS_RESUME_ENABLE,
                5,
                500,
                "",
                "",
                Unpooled.EMPTY_BUFFER,
                Unpooled.EMPTY_BUFFER));
  }

  @Test
  public void validResumeFrame() {
    ByteBuf token = Unpooled.wrappedBuffer(new byte[] {2, 3});
    ByteBuf metadata = Unpooled.wrappedBuffer(new byte[] {1, 2, 3, 4});
    ByteBuf data = Unpooled.wrappedBuffer(new byte[] {5, 4, 3});
    SetupFrameFlyweight.encode(
        byteBuf,
        SetupFrameFlyweight.FLAGS_RESUME_ENABLE,
        5,
        500,
        token,
        "metadata_type",
        "data_type",
        metadata,
        data);

    token.resetReaderIndex();
    metadata.resetReaderIndex();
    data.resetReaderIndex();

    assertEquals(FrameType.SETUP, FrameHeaderFlyweight.frameType(byteBuf));
    assertEquals("metadata_type", SetupFrameFlyweight.metadataMimeType(byteBuf));
    assertEquals("data_type", SetupFrameFlyweight.dataMimeType(byteBuf));
    assertEquals(metadata, FrameHeaderFlyweight.sliceFrameMetadata(byteBuf));
    assertEquals(data, FrameHeaderFlyweight.sliceFrameData(byteBuf));
    assertEquals(
        SetupFrameFlyweight.FLAGS_RESUME_ENABLE,
        FrameHeaderFlyweight.flags(byteBuf) & SetupFrameFlyweight.FLAGS_RESUME_ENABLE);
  }

  @Test
  public void testEncoding() {
    int encoded =
        SetupFrameFlyweight.encode(
            byteBuf,
            0,
            5000,
            60000,
            "mdmt",
            "dmt",
            Unpooled.copiedBuffer("md", StandardCharsets.UTF_8),
            Unpooled.copiedBuffer("d", StandardCharsets.UTF_8));
    assertEquals(
        "00002100000000050000010000000013880000ea60046d646d7403646d740000026d6464",
        ByteBufUtil.hexDump(byteBuf, 0, encoded));
  }
}
