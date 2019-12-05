package io.rsocket.frame;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.Throughput)
@Fork(
    value = 1 // , jvmArgsAppend = {"-Dio.netty.leakDetection.level=advanced"}
    )
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@State(Scope.Thread)
public class FrameTypePerf {
  @Benchmark
  public void lookup(Input input) {
    FrameType frameType = input.frameType;
    boolean b =
        frameType.canHaveData()
            && frameType.canHaveMetadata()
            && frameType.isFragmentable()
            && frameType.isRequestType()
            && frameType.hasInitialRequestN();

    input.bh.consume(b);
  }

  @State(Scope.Benchmark)
  public static class Input {
    Blackhole bh;
    FrameType frameType;

    @Setup
    public void setup(Blackhole bh) {
      this.bh = bh;
      this.frameType = FrameType.REQUEST_RESPONSE;
    }
  }
}
