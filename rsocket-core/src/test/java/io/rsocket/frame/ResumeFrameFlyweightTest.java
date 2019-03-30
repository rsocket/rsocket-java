/*
 * Copyright 2015-2019 the original author or authors.
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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

public class ResumeFrameFlyweightTest {

  @Test
  void testEncoding() {
    byte[] tokenBytes = new byte[65000];
    Arrays.fill(tokenBytes, (byte) 1);
    ByteBuf token = Unpooled.wrappedBuffer(tokenBytes);
    ByteBuf byteBuf = ResumeFrameFlyweight.encode(ByteBufAllocator.DEFAULT, token, 21, 12);
    Assert.assertEquals(
        ResumeFrameFlyweight.CURRENT_VERSION, ResumeFrameFlyweight.version(byteBuf));
    Assert.assertEquals(token, ResumeFrameFlyweight.token(byteBuf));
    Assert.assertEquals(21, ResumeFrameFlyweight.lastReceivedServerPos(byteBuf));
    Assert.assertEquals(12, ResumeFrameFlyweight.firstAvailableClientPos(byteBuf));
    byteBuf.release();
  }
}
