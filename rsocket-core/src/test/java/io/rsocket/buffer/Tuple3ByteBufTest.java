package io.rsocket.buffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

class Tuple3ByteBufTest {
  @Test
  void testTupleBufferGet() {
    ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
    ByteBuf one = allocator.directBuffer(9);

    byte[] bytes = new byte[9];
    ThreadLocalRandom.current().nextBytes(bytes);
    one.writeBytes(bytes);

    bytes = new byte[8];
    ThreadLocalRandom.current().nextBytes(bytes);
    ByteBuf two = Unpooled.wrappedBuffer(bytes);

    bytes = new byte[9];
    ThreadLocalRandom.current().nextBytes(bytes);
    ByteBuf three = Unpooled.wrappedBuffer(bytes);

    ByteBuf tuple = TupleByteBuf.of(one, two, three);

    int anInt = tuple.getInt(16);

    long aLong = tuple.getLong(15);

    short aShort = tuple.getShort(8);

    int medium = tuple.getMedium(8);
  }

  @Test
  void testTuple3BufferSlicing() {
    ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
    ByteBuf one = allocator.directBuffer();
    ByteBufUtil.writeUtf8(one, "foo");

    ByteBuf two = allocator.directBuffer();
    ByteBufUtil.writeUtf8(two, "bar");

    ByteBuf three = allocator.directBuffer();
    ByteBufUtil.writeUtf8(three, "bar");

    ByteBuf buf = TupleByteBuf.of(one, two, three);

    String s = buf.slice(0, 6).toString(Charset.defaultCharset());
    Assert.assertEquals("foobar", s);

    String s1 = buf.slice(3, 6).toString(Charset.defaultCharset());
    Assert.assertEquals("barbar", s1);

    String s2 = buf.slice(4, 4).toString(Charset.defaultCharset());
    Assert.assertEquals("arba", s2);
  }

  @Test
  void testTuple3ToNioBuffers() throws Exception {
    ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
    ByteBuf one = allocator.directBuffer();
    ByteBufUtil.writeUtf8(one, "one");

    ByteBuf two = allocator.directBuffer();
    ByteBufUtil.writeUtf8(two, "two");

    ByteBuf three = allocator.directBuffer();
    ByteBufUtil.writeUtf8(three, "three");

    ByteBuf buf = TupleByteBuf.of(one, two, three);
    ByteBuffer[] byteBuffers = buf.nioBuffers();

    Assert.assertEquals(3, byteBuffers.length);

    ByteBuffer bb = byteBuffers[0];
    byte[] dst = new byte[bb.remaining()];
    bb.get(dst);
    Assert.assertEquals("one", new String(dst, "UTF-8"));

    bb = byteBuffers[1];
    dst = new byte[bb.remaining()];
    bb.get(dst);
    Assert.assertEquals("two", new String(dst, "UTF-8"));

    bb = byteBuffers[2];
    dst = new byte[bb.remaining()];
    bb.get(dst);
    Assert.assertEquals("three", new String(dst, "UTF-8"));
  }
}
