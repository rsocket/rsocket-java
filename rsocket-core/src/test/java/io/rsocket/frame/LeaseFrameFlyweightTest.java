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

public class LeaseFrameFlyweightTest {
  private final ByteBuf byteBuf = Unpooled.buffer(1024);

  @Test
  public void size() {
    ByteBuf metadata = Unpooled.wrappedBuffer(new byte[] {1, 2, 3, 4});
    int length = LeaseFrameFlyweight.encode(byteBuf, 0, 0, metadata);
    assertEquals(length, 9 + 4 * 2 + 4); // Frame header + ttl + #requests + 4 byte metadata
  }

  @Test
  public void testEncoding() {
    int encoded =
        LeaseFrameFlyweight.encode(
            byteBuf, 0, 0, Unpooled.copiedBuffer("md", StandardCharsets.UTF_8));
    assertEquals(
        "00001000000000090000000000000000006d64", ByteBufUtil.hexDump(byteBuf, 0, encoded));
  }
}
