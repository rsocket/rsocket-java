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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

public class KeepaliveFrameFlyweightTest {
  private final ByteBuf byteBuf = Unpooled.buffer(1024);

  @Test
  public void canReadData() {
    ByteBuf data = Unpooled.wrappedBuffer(new byte[] {5, 4, 3});
    int length =
        KeepaliveFrameFlyweight.encode(byteBuf, KeepaliveFrameFlyweight.FLAGS_KEEPALIVE_R, data);
    data.resetReaderIndex();

    assertEquals(
        KeepaliveFrameFlyweight.FLAGS_KEEPALIVE_R,
        FrameHeaderFlyweight.flags(byteBuf) & KeepaliveFrameFlyweight.FLAGS_KEEPALIVE_R);
    assertEquals(data, FrameHeaderFlyweight.sliceFrameData(byteBuf));
  }

  @Test
  public void testEncoding() {
    int encoded =
        KeepaliveFrameFlyweight.encode(
            byteBuf,
            KeepaliveFrameFlyweight.FLAGS_KEEPALIVE_R,
            Unpooled.copiedBuffer("d", StandardCharsets.UTF_8));
    assertEquals("00000f000000000c80000000000000000064", ByteBufUtil.hexDump(byteBuf, 0, encoded));
  }
}
