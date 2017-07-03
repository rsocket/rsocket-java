package io.rsocket.resume;

import io.netty.buffer.ByteBufUtil;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

public final class ResumeToken {
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

  @Override public int hashCode() {
    return Arrays.hashCode(resumeToken);
  }

  @Override public boolean equals(Object obj) {
    if (obj instanceof ResumeToken) {
      return Arrays.equals(resumeToken, ((ResumeToken) obj).resumeToken);
    }

    return false;
  }

  @Override public String toString() {
    return ByteBufUtil.hexDump(resumeToken);
  }

  public byte[] toByteArray() {
    return resumeToken;
  }
}
