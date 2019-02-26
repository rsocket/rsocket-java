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

package io.rsocket.resume;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

public final class ResumeToken {
  private static final int MAX_TOKEN_LENGTH = 65535; // unsigned 16 bit
  private static final ResumeToken EMPTY_TOKEN = new ResumeToken(new byte[0]);
  private final byte[] resumeToken;

  protected ResumeToken(byte[] resumeToken) {
    assertToken(resumeToken);
    this.resumeToken = resumeToken;
  }

  public static ResumeToken fromBytes(@Nullable byte[] token) {
    return token == null || token.length == 0
        ? EMPTY_TOKEN : new ResumeToken(token);
  }

  public static ResumeToken empty() {
    return EMPTY_TOKEN;
  }

  public static ResumeToken generate() {
    return new ResumeToken(getBytesFromUUID(UUID.randomUUID()));
  }

  static byte[] getBytesFromUUID(UUID uuid) {
    ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
    bb.putLong(uuid.getMostSignificantBits());
    bb.putLong(uuid.getLeastSignificantBits());

    return bb.array();
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(resumeToken);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ResumeToken) {
      return Arrays.equals(resumeToken, ((ResumeToken) obj).resumeToken);
    }
    return false;
  }

  @Override
  public String toString() {
    return ByteBufUtil.hexDump(resumeToken);
  }

  public int length() {
    return resumeToken.length;
  }

  public boolean isEmpty() {
    return length() == 0;
  }

  public byte[] toByteArray() {
    return resumeToken;
  }

  public ByteBuf toByteBuf() {
    return isEmpty() ? Unpooled.EMPTY_BUFFER : Unpooled.wrappedBuffer(toByteArray());
  }

  private static void assertToken(byte[] resumeToken) {
    Objects.requireNonNull(resumeToken);
    int tokenLength = resumeToken.length;
    if (tokenLength > MAX_TOKEN_LENGTH) {
      throw new IllegalArgumentException(
          String.format(
              "Resumption token length exceeds limit of %d: %d", MAX_TOKEN_LENGTH, tokenLength));
    }
  }
}
