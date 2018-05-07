/*
 * Copyright 2015-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.fragmentation;

import static io.netty.buffer.UnpooledByteBufAllocator.DEFAULT;
import static io.rsocket.framing.RequestResponseFrame.createRequestResponseFrame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.rsocket.framing.Frame;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
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
import reactor.core.publisher.SynchronousSink;
import reactor.util.context.Context;

@BenchmarkMode(Mode.Throughput)
@Fork(
  value = 1 // , jvmArgsAppend = {"-Dio.netty.leakDetection.level=advanced"}
)
@Warmup(iterations = 10)
@Measurement(iterations = 10_000)
@State(Scope.Thread)
public class FragmentationPerformanceTest {

  @Benchmark
  public void largeFragmentation(Input input) {
    Frame frame =
        input.largeFragmenter.fragment(input.largeFrame).doOnNext(Frame::dispose).blockLast();

    input.bh.consume(frame);
  }

  @Benchmark
  public void largeReassembly(Input input) {
    input.largeFrames.forEach(frame -> input.reassembler.reassemble(frame));

    input.bh.consume(input.sink.next);
  }

  @Benchmark
  public void smallFragmentation(Input input) {
    Frame frame =
        input.smallFragmenter.fragment(input.smallFrame).doOnNext(Frame::dispose).blockLast();

    input.bh.consume(frame);
  }

  @Benchmark
  public void smallReassembly(Input input) {
    input.smallFrames.forEach(frame -> input.reassembler.reassemble(frame));

    input.bh.consume(input.sink.next);
  }

  @State(Scope.Benchmark)
  public static class Input {

    Blackhole bh;

    FrameFragmenter largeFragmenter;

    Frame largeFrame;

    List<Frame> largeFrames;

    FrameReassembler reassembler = FrameReassembler.createFrameReassembler(DEFAULT);

    MockSynchronousSink<Frame> sink;

    FrameFragmenter smallFragmenter;

    Frame smallFrame;

    List<Frame> smallFrames;

    @Setup
    public void setup(Blackhole bh) {
      this.bh = bh;

      sink = new MockSynchronousSink<>();

      largeFrame =
          createRequestResponseFrame(
              DEFAULT, false, getRandomByteBuf(1 << 18), getRandomByteBuf(1 << 18));

      smallFrame =
          createRequestResponseFrame(DEFAULT, false, getRandomByteBuf(16), getRandomByteBuf(16));

      largeFragmenter = new FrameFragmenter(DEFAULT, 1024);
      smallFragmenter = new FrameFragmenter(ByteBufAllocator.DEFAULT, 2);

      largeFrames = largeFragmenter.fragment(largeFrame).collectList().block();
      smallFrames = smallFragmenter.fragment(smallFrame).collectList().block();
    }

    private static ByteBuf getRandomByteBuf(int size) {
      byte[] bytes = new byte[size];
      ThreadLocalRandom.current().nextBytes(bytes);
      return Unpooled.wrappedBuffer(bytes);
    }
  }

  static final class MockSynchronousSink<T> implements SynchronousSink<T> {

    Throwable error;

    T next;

    @Override
    public void complete() {}

    @Override
    public Context currentContext() {
      return null;
    }

    @Override
    public void error(Throwable e) {
      this.error = e;
    }

    @Override
    public void next(T t) {
      this.next = t;
    }
  }
}
