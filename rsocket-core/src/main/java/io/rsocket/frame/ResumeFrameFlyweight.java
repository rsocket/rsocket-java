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

public class ResumeFrameFlyweight {
  static final int CURRENT_VERSION = SetupFrameFlyweight.CURRENT_VERSION;

  public static ByteBuf encode(
      final ByteBufAllocator allocator,
      byte[] token,
      long lastReceivedServerPos,
      long firstAvailableClientPos) {

    ByteBuf byteBuf = FrameHeaderFlyweight.encodeStreamZero(allocator, FrameType.RESUME, 0);
    byteBuf.writeInt(CURRENT_VERSION);
    byteBuf.writeShort(token.length);
    byteBuf.writeBytes(token);
    byteBuf.writeLong(lastReceivedServerPos);
    byteBuf.writeLong(firstAvailableClientPos);

    return byteBuf;
  }

  public static int version(ByteBuf byteBuf) {
    FrameHeaderFlyweight.ensureFrameType(FrameType.RESUME, byteBuf);

    byteBuf.markReaderIndex();
    byteBuf.skipBytes(FrameHeaderFlyweight.size());
    int version = byteBuf.readInt();
    byteBuf.resetReaderIndex();

    return version;
  }

  public static byte[] token(ByteBuf byteBuf) {
    FrameHeaderFlyweight.ensureFrameType(FrameType.RESUME, byteBuf);

    byteBuf.markReaderIndex();
    // header + version
    int tokenPos = FrameHeaderFlyweight.size() + Integer.BYTES;
    byteBuf.skipBytes(tokenPos);
    //token
    int tokenLength = byteBuf.readShort() & 0xFFFF;
    byte[] token = new byte[tokenLength];
    byteBuf.readBytes(token);
    byteBuf.resetReaderIndex();

    return token;
  }

  public static long lastReceivedServerPos(ByteBuf byteBuf) {
    FrameHeaderFlyweight.ensureFrameType(FrameType.RESUME, byteBuf);

    byteBuf.markReaderIndex();
    // header + version
    int tokenPos = FrameHeaderFlyweight.size() + Integer.BYTES;
    byteBuf.skipBytes(tokenPos);
    //token
    int tokenLength = byteBuf.readShort() & 0xFFFF;
    byteBuf.skipBytes(tokenLength);
    long lastReceivedServerPos = byteBuf.readLong();
    byteBuf.resetReaderIndex();

    return lastReceivedServerPos;
  }

  public static long firstAvailableClientPos(ByteBuf byteBuf) {
    FrameHeaderFlyweight.ensureFrameType(FrameType.RESUME, byteBuf);

    byteBuf.markReaderIndex();
    // header + version
    int tokenPos = FrameHeaderFlyweight.size() + Integer.BYTES;
    byteBuf.skipBytes(tokenPos);
    //token
    int tokenLength = byteBuf.readShort() & 0xFFFF;
    byteBuf.skipBytes(tokenLength);
    // last received server position
    byteBuf.skipBytes(Long.BYTES);
    long firstAvailableClientPos = byteBuf.readLong();
    byteBuf.resetReaderIndex();

    return firstAvailableClientPos;
  }
}
