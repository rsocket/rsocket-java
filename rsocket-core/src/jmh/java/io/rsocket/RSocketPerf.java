package io.rsocket;

import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.local.LocalClientTransport;
import io.rsocket.transport.local.LocalServerTransport;
import io.rsocket.util.EmptyPayload;
import java.util.stream.IntStream;
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
import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@BenchmarkMode(Mode.Throughput)
@Fork(
    value = 1 //, jvmArgsAppend = {"-Dio.netty.leakDetection.level=advanced"}
    )
@Warmup(iterations = 10)
@Measurement(iterations = 10, time = 10)
@State(Scope.Benchmark)
public class RSocketPerf {

  static final Payload PAYLOAD = EmptyPayload.INSTANCE;
  static final Mono<Payload> PAYLOAD_MONO = Mono.just(PAYLOAD);
  static final Flux<Payload> PAYLOAD_FLUX =
      Flux.fromArray(IntStream.range(0, 100000).mapToObj(__ -> PAYLOAD).toArray(Payload[]::new));

  RSocket client;
  Closeable server;

  @Setup
  public void setUp() {
    server =
        RSocketFactory.receive()
            .acceptor(
                (setup, sendingSocket) ->
                    Mono.just(
                        new AbstractRSocket() {

                          @Override
                          public Mono<Void> fireAndForget(Payload payload) {
                            payload.release();
                            return Mono.empty();
                          }

                          @Override
                          public Mono<Payload> requestResponse(Payload payload) {
                            payload.release();
                            return PAYLOAD_MONO;
                          }

                          @Override
                          public Flux<Payload> requestStream(Payload payload) {
                            payload.release();
                            return PAYLOAD_FLUX;
                          }

                          @Override
                          public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
                            return Flux.from(payloads);
                          }
                        }))
            .transport(LocalServerTransport.create("server"))
            .start()
            .block();

    client =
        RSocketFactory.connect()
            .frameDecoder(PayloadDecoder.ZERO_COPY)
            .transport(LocalClientTransport.create("server"))
            .start()
            .block();
  }

  @Benchmark
  @SuppressWarnings("unchecked")
  public PerfSubscriber fireAndForget(Blackhole blackhole) throws InterruptedException {
    PerfSubscriber subscriber = new PerfSubscriber(blackhole);
    client.fireAndForget(PAYLOAD).subscribe((CoreSubscriber) subscriber);
    subscriber.latch.await();

    return subscriber;
  }

  @Benchmark
  public PerfSubscriber requestResponse(Blackhole blackhole) throws InterruptedException {
    PerfSubscriber subscriber = new PerfSubscriber(blackhole);
    client.requestResponse(PAYLOAD).subscribe(subscriber);
    subscriber.latch.await();

    return subscriber;
  }

  @Benchmark
  public PerfSubscriber requestStreamWithRequestByOneStrategy(Blackhole blackhole)
      throws InterruptedException {
    PerfSubscriber subscriber = new PerfSubscriber(blackhole);
    client.requestStream(PAYLOAD).subscribe(subscriber);
    subscriber.latch.await();

    return subscriber;
  }

  @Benchmark
  public MaxPerfSubscriber requestStreamWithRequestAllStrategy(Blackhole blackhole)
      throws InterruptedException {
    MaxPerfSubscriber subscriber = new MaxPerfSubscriber(blackhole);
    client.requestStream(PAYLOAD).subscribe(subscriber);
    subscriber.latch.await();

    return subscriber;
  }

  @Benchmark
  public PerfSubscriber requestChannelWithRequestByOneStrategy(Blackhole blackhole)
      throws InterruptedException {
    PerfSubscriber subscriber = new PerfSubscriber(blackhole);
    client.requestChannel(PAYLOAD_FLUX).subscribe(subscriber);
    subscriber.latch.await();

    return subscriber;
  }

  @Benchmark
  public MaxPerfSubscriber requestChannelWithRequestAllStrategy(Blackhole blackhole)
      throws InterruptedException {
    MaxPerfSubscriber subscriber = new MaxPerfSubscriber(blackhole);
    client.requestChannel(PAYLOAD_FLUX).subscribe(subscriber);
    subscriber.latch.await();

    return subscriber;
  }
}
