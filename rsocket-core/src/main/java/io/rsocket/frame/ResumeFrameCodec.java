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
import java.util.UUID;

public class ResumeFrameCodec {
  static final int CURRENT_VERSION = SetupFrameCodec.CURRENT_VERSION;

  public static ByteBuf encode(
      ByteBufAllocator allocator,
      ByteBuf token,
      long lastReceivedServerPos,
      long firstAvailableClientPos) {

    ByteBuf byteBuf = FrameHeaderCodec.encodeStreamZero(allocator, FrameType.RESUME, 0);
    byteBuf.writeInt(CURRENT_VERSION);
    token.markReaderIndex();
    byteBuf.writeShort(token.readableBytes());
    byteBuf.writeBytes(token);
    token.resetReaderIndex();
    byteBuf.writeLong(lastReceivedServerPos);
    byteBuf.writeLong(firstAvailableClientPos);

    return byteBuf;
  }

  public static int version(ByteBuf byteBuf) {
    FrameHeaderCodec.ensureFrameType(FrameType.RESUME, byteBuf);

    byteBuf.markReaderIndex();
    byteBuf.skipBytes(FrameHeaderCodec.size());
    int version = byteBuf.readInt();
    byteBuf.resetReaderIndex();

    return version;
  }

  public static ByteBuf token(ByteBuf byteBuf) {
    FrameHeaderCodec.ensureFrameType(FrameType.RESUME, byteBuf);

    byteBuf.markReaderIndex();
    // header + version
    int tokenPos = FrameHeaderCodec.size() + Integer.BYTES;
    byteBuf.skipBytes(tokenPos);
    // token
    int tokenLength = byteBuf.readShort() & 0xFFFF;
    ByteBuf token = byteBuf.readSlice(tokenLength);
    byteBuf.resetReaderIndex();

    return token;
  }

  public static long lastReceivedServerPos(ByteBuf byteBuf) {
    FrameHeaderCodec.ensureFrameType(FrameType.RESUME, byteBuf);

    byteBuf.markReaderIndex();
    // header + version
    int tokenPos = FrameHeaderCodec.size() + Integer.BYTES;
    byteBuf.skipBytes(tokenPos);
    // token
    int tokenLength = byteBuf.readShort() & 0xFFFF;
    byteBuf.skipBytes(tokenLength);
    long lastReceivedServerPos = byteBuf.readLong();
    byteBuf.resetReaderIndex();

    return lastReceivedServerPos;
  }

  public static long firstAvailableClientPos(ByteBuf byteBuf) {
    FrameHeaderCodec.ensureFrameType(FrameType.RESUME, byteBuf);

    byteBuf.markReaderIndex();
    // header + version
    int tokenPos = FrameHeaderCodec.size() + Integer.BYTES;
    byteBuf.skipBytes(tokenPos);
    // token
    int tokenLength = byteBuf.readShort() & 0xFFFF;
    byteBuf.skipBytes(tokenLength);
    // last received server position
    byteBuf.skipBytes(Long.BYTES);
    long firstAvailableClientPos = byteBuf.readLong();
    byteBuf.resetReaderIndex();

    return firstAvailableClientPos;
  }

  public static ByteBuf generateResumeToken() {
    UUID uuid = UUID.randomUUID();
    ByteBuf bb = Unpooled.buffer(16);
    bb.writeLong(uuid.getMostSignificantBits());
    bb.writeLong(uuid.getLeastSignificantBits());
    return bb;
  }
}
