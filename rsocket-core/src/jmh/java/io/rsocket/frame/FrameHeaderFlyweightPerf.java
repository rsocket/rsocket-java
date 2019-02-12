package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.Throughput)
@Fork(
    value = 1 // , jvmArgsAppend = {"-Dio.netty.leakDetection.level=advanced"}
    )
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@State(Scope.Thread)
public class FrameHeaderFlyweightPerf {

  @Benchmark
  public void encode(Input input) {
    ByteBuf byteBuf = FrameHeaderFlyweight.encodeStreamZero(input.allocator, FrameType.SETUP, 0);
    boolean release = byteBuf.release();
    input.bh.consume(release);
  }

  @Benchmark
  public void decode(Input input) {
    ByteBuf frame = input.frame;
    FrameType frameType = FrameHeaderFlyweight.frameType(frame);
    int streamId = FrameHeaderFlyweight.streamId(frame);
    int flags = FrameHeaderFlyweight.flags(frame);
    input.bh.consume(streamId);
    input.bh.consume(flags);
    input.bh.consume(frameType);
  }

  @State(Scope.Benchmark)
  public static class Input {
    Blackhole bh;
    FrameType frameType;
    ByteBufAllocator allocator;
    ByteBuf frame;

    @Setup
    public void setup(Blackhole bh) {
      this.bh = bh;
      this.frameType = FrameType.REQUEST_RESPONSE;
      allocator = ByteBufAllocator.DEFAULT;
      frame = FrameHeaderFlyweight.encode(allocator, 123, FrameType.SETUP, 0);
    }

    @TearDown
    public void teardown() {
      frame.release();
    }
  }
}
