package io.rsocket.buffer;

import static io.netty.buffer.Unpooled.wrappedBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class Tuple3ByteBufTest extends AbstractByteBufTest {

  @Parameterized.Parameters
  public static Object[] data() {
    return new Object[] {
      (Function<Integer, ByteBuf[]>)
          (Integer capacity) -> {
            int i = capacity / 3;
            return new ByteBuf[] {
              wrappedBuffer(new byte[i]),
              wrappedBuffer(new byte[capacity - i - i]),
              wrappedBuffer(new byte[i]),
            };
          },
      (Function<Integer, ByteBuf[]>)
          (Integer capacity) -> {
            int i =
                Math.min(
                    Math.max(capacity / (Long.SIZE + Short.SIZE), 1), Math.max(capacity / 3, 1));

            int capacity1 = i;
            int capacity2 = Math.max(Math.min((capacity - capacity1) / 3, i / 2), 0);
            int capacity3 = Math.max(capacity - capacity1 - capacity2, 0);

            return new ByteBuf[] {
              wrappedBuffer(new byte[capacity1]),
              wrappedBuffer(new byte[capacity2]),
              wrappedBuffer(new byte[capacity3])
            };
          },
      (Function<Integer, ByteBuf[]>)
          (Integer capacity) -> {
            return new ByteBuf[] {
              wrappedBuffer(new byte[0]),
              wrappedBuffer(new byte[0]),
              wrappedBuffer(new byte[capacity])
            };
          },
      (Function<Integer, ByteBuf[]>)
          (Integer capacity) -> {
            return new ByteBuf[] {
              wrappedBuffer(new byte[0]),
              wrappedBuffer(new byte[capacity]),
              wrappedBuffer(new byte[0])
            };
          },
      (Function<Integer, ByteBuf[]>)
          (Integer capacity) -> {
            return new ByteBuf[] {
              wrappedBuffer(new byte[capacity]),
              wrappedBuffer(new byte[0]),
              wrappedBuffer(new byte[0])
            };
          }
    };
  }

  private final Function<Integer, ByteBuf[]> innerBuffersGenerator;

  public Tuple3ByteBufTest(Object innerBuffersGenerator) {
    this.innerBuffersGenerator = (Function<Integer, ByteBuf[]>) innerBuffersGenerator;
  }

  @Override
  protected ByteBuf[] newInnerBuffers(int capacity, int maxCapacity) {
    return innerBuffersGenerator.apply(capacity);
  }

  @Override
  protected AbstractTupleByteBuf newBuffer(ByteBuf[] inners) {
    return (AbstractTupleByteBuf) TupleByteBuf.of(inners[0], inners[1], inners[2]);
  }

  @Test
  public void testTupleBufferGet() {
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
  public void testTuple3BufferSlicing() {
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
  public void testTuple3ToNioBuffers() throws Exception {
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
