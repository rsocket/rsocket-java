package io.rsocket;

import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.local.LocalClientTransport;
import io.rsocket.transport.local.LocalServerTransport;
import io.rsocket.util.EmptyPayload;
import java.lang.reflect.Field;
import java.util.Queue;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.IntStream;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@BenchmarkMode(Mode.Throughput)
@Fork(
    value = 1 // , jvmArgsAppend = {"-Dio.netty.leakDetection.level=advanced"}
    )
@Warmup(iterations = 10)
@Measurement(iterations = 10, time = 20)
@State(Scope.Benchmark)
public class RSocketPerf {

  static final Payload PAYLOAD = EmptyPayload.INSTANCE;
  static final Mono<Payload> PAYLOAD_MONO = Mono.just(PAYLOAD);
  static final Flux<Payload> PAYLOAD_FLUX =
      Flux.fromArray(IntStream.range(0, 100000).mapToObj(__ -> PAYLOAD).toArray(Payload[]::new));

  RSocket client;
  Closeable server;
  Queue clientsQueue;

  @TearDown
  public void tearDown() {
    client.dispose();
    server.dispose();
  }

  @TearDown(Level.Iteration)
  public void awaitToBeConsumed() {
    while (!clientsQueue.isEmpty()) {
      LockSupport.parkNanos(1000);
    }
  }

  @Setup
  public void setUp() throws NoSuchFieldException, IllegalAccessException {
    server =
        RSocketFactory.receive()
            .frameDecoder(PayloadDecoder.ZERO_COPY)
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
            .singleSubscriberRequester()
            .frameDecoder(PayloadDecoder.ZERO_COPY)
            .transport(LocalClientTransport.create("server"))
            .start()
            .block();

    Field sendProcessorField = RSocketRequester.class.getDeclaredField("sendProcessor");
    sendProcessorField.setAccessible(true);

    clientsQueue = (Queue) sendProcessorField.get(client);
  }

  @Benchmark
  @SuppressWarnings("unchecked")
  public PayloadsPerfSubscriber fireAndForget(Blackhole blackhole) throws InterruptedException {
    PayloadsPerfSubscriber subscriber = new PayloadsPerfSubscriber(blackhole);
    client.fireAndForget(PAYLOAD).subscribe((CoreSubscriber) subscriber);
    subscriber.latch.await();

    return subscriber;
  }

  @Benchmark
  public PayloadsPerfSubscriber requestResponse(Blackhole blackhole) throws InterruptedException {
    PayloadsPerfSubscriber subscriber = new PayloadsPerfSubscriber(blackhole);
    client.requestResponse(PAYLOAD).subscribe(subscriber);
    subscriber.latch.await();

    return subscriber;
  }

  @Benchmark
  public PayloadsPerfSubscriber requestStreamWithRequestByOneStrategy(Blackhole blackhole)
      throws InterruptedException {
    PayloadsPerfSubscriber subscriber = new PayloadsPerfSubscriber(blackhole);
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
  public PayloadsPerfSubscriber requestChannelWithRequestByOneStrategy(Blackhole blackhole)
      throws InterruptedException {
    PayloadsPerfSubscriber subscriber = new PayloadsPerfSubscriber(blackhole);
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
