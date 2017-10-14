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
import org.junit.jupiter.api.Test;

public class RequestNFrameFlyweightTest {
  private final ByteBuf byteBuf = Unpooled.buffer(1024);

  @Test
  public void testEncoding() {
    int encoded = RequestNFrameFlyweight.encode(byteBuf, 1, 5);
    assertEquals("00000a00000001200000000005", ByteBufUtil.hexDump(byteBuf, 0, encoded));
  }
}
