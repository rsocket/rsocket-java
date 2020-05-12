package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.Throughput)
@Fork(
    value = 1 // , jvmArgsAppend = {"-Dio.netty.leakDetection.level=advanced"}
    )
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@State(Scope.Thread)
public class PayloadFlyweightPerf {

  @Benchmark
  public void encode(Input input) {
    ByteBuf encode =
        PayloadFrameCodec.encode(
            input.allocator,
            100,
            false,
            true,
            false,
            Unpooled.wrappedBuffer(input.metadata),
            Unpooled.wrappedBuffer(input.data));
    boolean release = encode.release();
    input.bh.consume(release);
  }

  @Benchmark
  public void decode(Input input) {
    ByteBuf frame = input.payload;
    ByteBuf data = PayloadFrameCodec.data(frame);
    ByteBuf metadata = PayloadFrameCodec.metadata(frame);
    input.bh.consume(data);
    input.bh.consume(metadata);
  }

  @State(Scope.Benchmark)
  public static class Input {
    Blackhole bh;
    FrameType frameType;
    ByteBufAllocator allocator;
    ByteBuf payload;
    byte[] metadata = new byte[512];
    byte[] data = new byte[4096];

    @Setup
    public void setup(Blackhole bh) {
      this.bh = bh;
      this.frameType = FrameType.REQUEST_RESPONSE;
      allocator = ByteBufAllocator.DEFAULT;

      // Encode a payload and then copy it a single bytebuf
      payload = allocator.buffer();
      ByteBuf encode =
          PayloadFrameCodec.encode(
              allocator,
              100,
              false,
              true,
              false,
              Unpooled.wrappedBuffer(metadata),
              Unpooled.wrappedBuffer(data));
      payload.writeBytes(encode);
      encode.release();
    }

    @TearDown
    public void teardown() {
      payload.release();
    }
  }
}
