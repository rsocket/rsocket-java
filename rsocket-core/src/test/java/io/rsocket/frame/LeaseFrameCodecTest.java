package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LeaseFrameCodecTest {

  @Test
  void leaseMetadata() {
    ByteBuf metadata = bytebuf("md");
    int ttl = 1;
    int numRequests = 42;
    ByteBuf lease = LeaseFrameCodec.encode(ByteBufAllocator.DEFAULT, ttl, numRequests, metadata);

    Assertions.assertTrue(FrameHeaderCodec.hasMetadata(lease));
    Assertions.assertEquals(ttl, LeaseFrameCodec.ttl(lease));
    Assertions.assertEquals(numRequests, LeaseFrameCodec.numRequests(lease));
    Assertions.assertEquals(metadata, LeaseFrameCodec.metadata(lease));
    lease.release();
  }

  @Test
  void leaseAbsentMetadata() {
    int ttl = 1;
    int numRequests = 42;
    ByteBuf lease = LeaseFrameCodec.encode(ByteBufAllocator.DEFAULT, ttl, numRequests, null);

    Assertions.assertFalse(FrameHeaderCodec.hasMetadata(lease));
    Assertions.assertEquals(ttl, LeaseFrameCodec.ttl(lease));
    Assertions.assertEquals(numRequests, LeaseFrameCodec.numRequests(lease));
    Assertions.assertNull(LeaseFrameCodec.metadata(lease));
    lease.release();
  }

  private static ByteBuf bytebuf(String str) {
    return Unpooled.copiedBuffer(str, StandardCharsets.UTF_8);
  }
}
