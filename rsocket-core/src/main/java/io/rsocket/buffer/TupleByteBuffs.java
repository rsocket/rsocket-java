package io.rsocket.buffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.util.Objects;

public abstract class TupleByteBuffs {

  private TupleByteBuffs() {}

  public static ByteBuf of(ByteBuf one, ByteBuf two) {
    return of(ByteBufAllocator.DEFAULT, one, two);
  }

  public static ByteBuf of(ByteBufAllocator allocator, ByteBuf one, ByteBuf two) {
    Objects.requireNonNull(allocator);
    Objects.requireNonNull(one);
    Objects.requireNonNull(two);

    return new Tuple2ByteBuf(allocator, one, two);
  }

  public static ByteBuf of(ByteBuf one, ByteBuf two, ByteBuf three) {
    return of(ByteBufAllocator.DEFAULT, one, two, three);
  }

  public static ByteBuf of(ByteBufAllocator allocator, ByteBuf one, ByteBuf two, ByteBuf three) {
    Objects.requireNonNull(allocator);
    Objects.requireNonNull(one);
    Objects.requireNonNull(two);
    Objects.requireNonNull(three);

    return new Tuple3ByteBuf(allocator, one, two, three);
  }
}
