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

package io.rsocket.frame;

import static io.rsocket.frame.ErrorFrameFlyweight.*;
import static org.junit.Assert.assertEquals;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.rsocket.exceptions.*;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

public class ErrorFrameFlyweightTest {
  private final ByteBuf byteBuf = Unpooled.buffer(1024);

  @Test
  public void testEncoding() {
    int encoded =
        ErrorFrameFlyweight.encode(
            byteBuf,
            1,
            ErrorFrameFlyweight.APPLICATION_ERROR,
            Unpooled.copiedBuffer("d", StandardCharsets.UTF_8));
    assertEquals("00000b000000012c000000020164", ByteBufUtil.hexDump(byteBuf, 0, encoded));

    assertEquals(ErrorFrameFlyweight.APPLICATION_ERROR, ErrorFrameFlyweight.errorCode(byteBuf));
    assertEquals("d", ErrorFrameFlyweight.message(byteBuf));
  }
}
