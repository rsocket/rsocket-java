package io.rsocket;

import io.rsocket.fragmentation.FrameFragmenter;
import io.rsocket.fragmentation.FrameReassembler;
import io.rsocket.util.PayloadImpl;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.Throughput)
@Fork(
  value = 1 // , jvmArgsAppend = {"-Dio.netty.leakDetection.level=advanced"}
)
@Warmup(iterations = 10)
@Measurement(iterations = 10_000)
@State(Scope.Thread)
public class FragmentationPerf {
  @State(Scope.Benchmark)
  public static class Input {
    Blackhole bh;
    Frame smallFrame;
    FrameFragmenter smallFrameFragmenter;

    Frame largeFrame;
    FrameFragmenter largeFrameFragmenter;

    Iterable<Frame> smallFramesIterable;

    @Setup
    public void setup(Blackhole bh) {
      this.bh = bh;

      ByteBuffer data = createRandomBytes(1 << 18);
      ByteBuffer metadata = createRandomBytes(1 << 18);
      largeFrame =
          Frame.Request.from(1, FrameType.REQUEST_RESPONSE, new PayloadImpl(data, metadata), 1);
      largeFrameFragmenter = new FrameFragmenter(1024);

      data = createRandomBytes(16);
      metadata = createRandomBytes(16);
      smallFrame =
          Frame.Request.from(1, FrameType.REQUEST_RESPONSE, new PayloadImpl(data, metadata), 1);
      smallFrameFragmenter = new FrameFragmenter(2);
      smallFramesIterable =
          smallFrameFragmenter
              .fragment(smallFrame)
              .map(Frame::copy)
              .toStream()
              .collect(Collectors.toList());
    }
  }

  @Benchmark
  public void smallFragmentationPerf(Input input) {
    Frame frame =
        input.smallFrameFragmenter.fragment(input.smallFrame).doOnNext(Frame::release).blockLast();
    input.bh.consume(frame);
  }

  @Benchmark
  public void largeFragmentationPerf(Input input) {
    Frame frame =
        input.largeFrameFragmenter.fragment(input.largeFrame).doOnNext(Frame::release).blockLast();
    input.bh.consume(frame);
  }

  @Benchmark
  public void smallFragmentationFrameReassembler(Input input) {
    FrameReassembler smallFragmentAssembler = new FrameReassembler(input.smallFrame);

    input.smallFramesIterable.forEach(smallFragmentAssembler::append);

    Frame frame = smallFragmentAssembler.reassemble();
    input.bh.consume(frame);
    frame.release();
    // input.smallFragmentAssembler.clear();
  }

  private static ByteBuffer createRandomBytes(int size) {
    byte[] bytes = new byte[size];
    ThreadLocalRandom.current().nextBytes(bytes);
    return ByteBuffer.wrap(bytes);
  }
}
