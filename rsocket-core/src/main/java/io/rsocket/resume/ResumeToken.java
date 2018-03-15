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

import io.netty.buffer.ByteBufUtil;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

public final class ResumeToken {
  // TODO consider best format to store this
  private final byte[] resumeToken;

  protected ResumeToken(byte[] resumeToken) {
    this.resumeToken = resumeToken;
  }

  public static ResumeToken bytes(byte[] token) {
    return new ResumeToken(token);
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

  public byte[] toByteArray() {
    return resumeToken;
  }
}
