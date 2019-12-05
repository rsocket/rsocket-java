package io.rsocket.internal;

import io.rsocket.MaxPerfSubscriber;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import reactor.core.publisher.MonoProcessor;

@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
@Fork(1)
@Warmup(iterations = 10)
@Measurement(iterations = 10, time = 20)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class UnicastVsDefaultMonoProcessorPerf {

  @Benchmark
  public void monoProcessorPerf(Blackhole bh) {
    MaxPerfSubscriber<Integer> subscriber = new MaxPerfSubscriber<>(bh);
    MonoProcessor<Integer> monoProcessor = MonoProcessor.create();
    monoProcessor.onNext(1);
    monoProcessor.subscribe(subscriber);

    bh.consume(monoProcessor);
    bh.consume(subscriber);
  }

  @Benchmark
  public void unicastMonoProcessorPerf(Blackhole bh) {
    MaxPerfSubscriber<Integer> subscriber = new MaxPerfSubscriber<>(bh);
    UnicastMonoProcessor<Integer> monoProcessor = UnicastMonoProcessor.create();
    monoProcessor.onNext(1);
    monoProcessor.subscribe(subscriber);

    bh.consume(monoProcessor);
    bh.consume(subscriber);
  }
}
