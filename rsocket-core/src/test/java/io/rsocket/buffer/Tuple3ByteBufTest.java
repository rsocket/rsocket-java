package io.rsocket.buffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.util.concurrent.ThreadLocalRandom;
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
}
