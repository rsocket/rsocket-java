package io.rsocket;

import io.netty.util.collection.IntObjectMap;
import io.rsocket.internal.SynchronizedIntObjectHashMap;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.Throughput)
@Fork(
    value = 1 // , jvmArgsAppend = {"-Dio.netty.leakDetection.level=advanced"}
    )
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@State(Scope.Thread)
public class StreamIdSupplierPerf {
  @Benchmark
  public void benchmarkStreamId(Input input) {
    int i = input.supplier.nextStreamId(input.map);
    input.bh.consume(i);
  }

  @State(Scope.Benchmark)
  public static class Input {
    Blackhole bh;
    IntObjectMap map;
    StreamIdSupplier supplier;

    @Setup
    public void setup(Blackhole bh) {
      this.supplier = StreamIdSupplier.clientSupplier();
      this.bh = bh;
      this.map = new SynchronizedIntObjectHashMap();
    }
  }
}
