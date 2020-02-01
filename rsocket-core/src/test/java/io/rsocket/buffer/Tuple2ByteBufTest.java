package io.rsocket.buffer;

import static io.netty.buffer.Unpooled.wrappedBuffer;

import io.netty.buffer.ByteBuf;
import java.util.function.Function;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class Tuple2ByteBufTest extends AbstractByteBufTest {

  @Parameterized.Parameters
  public static Object[] data() {
    return new Object[] {
      (Function<Integer, ByteBuf[]>)
          (Integer capacity) -> {
            int i = capacity / 2;
            return new ByteBuf[] {
              wrappedBuffer(new byte[i]), wrappedBuffer(new byte[capacity - i])
            };
          },
      (Function<Integer, ByteBuf[]>)
          (Integer capacity) -> {
            int i =
                Math.min(
                    Math.max(capacity / (Long.SIZE + Short.SIZE), 1), Math.max(capacity / 2, 1));

            return new ByteBuf[] {
              wrappedBuffer(new byte[i]), wrappedBuffer(new byte[Math.max(capacity - i, 0)])
            };
          },
      (Function<Integer, ByteBuf[]>)
          (Integer capacity) -> {
            int i =
                Math.min(
                    Math.max(capacity / (Long.SIZE + Short.SIZE), 1), Math.max(capacity / 2, 1));

            return new ByteBuf[] {
              wrappedBuffer(new byte[Math.max(capacity - i, 0)]), wrappedBuffer(new byte[i]),
            };
          },
      (Function<Integer, ByteBuf[]>)
          (Integer capacity) -> {
            return new ByteBuf[] {
              wrappedBuffer(new byte[0]), wrappedBuffer(new byte[capacity]),
            };
          },
      (Function<Integer, ByteBuf[]>)
          (Integer capacity) -> {
            return new ByteBuf[] {
              wrappedBuffer(new byte[capacity]), wrappedBuffer(new byte[0]),
            };
          }
    };
  }

  private final Function<Integer, ByteBuf[]> innerBuffersGenerator;

  public Tuple2ByteBufTest(Object innerBuffersGenerator) {
    this.innerBuffersGenerator = (Function<Integer, ByteBuf[]>) innerBuffersGenerator;
  }

  @Override
  protected ByteBuf[] newInnerBuffers(int capacity, int maxCapacity) {
    return innerBuffersGenerator.apply(capacity);
  }

  @Override
  protected AbstractTupleByteBuf newBuffer(ByteBuf[] inners) {
    return (AbstractTupleByteBuf) TupleByteBuf.of(inners[0], inners[1]);
  }
}
