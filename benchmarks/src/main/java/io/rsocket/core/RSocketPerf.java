package io.rsocket.core;

import io.rsocket.AbstractRSocket;
import io.rsocket.Closeable;
import io.rsocket.Payload;
import io.rsocket.PayloadsMaxPerfSubscriber;
import io.rsocket.PayloadsPerfSubscriber;
import io.rsocket.RSocket;
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
        RSocketServer.create(
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
            .payloadDecoder(PayloadDecoder.ZERO_COPY)
            .bind(LocalServerTransport.create("server"))
            .block();

    client =
        RSocketConnector.create()
            .payloadDecoder(PayloadDecoder.ZERO_COPY)
            .connect(LocalClientTransport.create("server"))
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
    subscriber.await();

    return subscriber;
  }

  @Benchmark
  public PayloadsPerfSubscriber requestResponse(Blackhole blackhole) throws InterruptedException {
    PayloadsPerfSubscriber subscriber = new PayloadsPerfSubscriber(blackhole);
    client.requestResponse(PAYLOAD).subscribe(subscriber);
    subscriber.await();

    return subscriber;
  }

  @Benchmark
  public PayloadsPerfSubscriber requestStreamWithRequestByOneStrategy(Blackhole blackhole)
      throws InterruptedException {
    PayloadsPerfSubscriber subscriber = new PayloadsPerfSubscriber(blackhole);
    client.requestStream(PAYLOAD).subscribe(subscriber);
    subscriber.await();

    return subscriber;
  }

  @Benchmark
  public PayloadsMaxPerfSubscriber requestStreamWithRequestAllStrategy(Blackhole blackhole)
      throws InterruptedException {
    PayloadsMaxPerfSubscriber subscriber = new PayloadsMaxPerfSubscriber(blackhole);
    client.requestStream(PAYLOAD).subscribe(subscriber);
    subscriber.await();

    return subscriber;
  }

  @Benchmark
  public PayloadsPerfSubscriber requestChannelWithRequestByOneStrategy(Blackhole blackhole)
      throws InterruptedException {
    PayloadsPerfSubscriber subscriber = new PayloadsPerfSubscriber(blackhole);
    client.requestChannel(PAYLOAD_FLUX).subscribe(subscriber);
    subscriber.await();

    return subscriber;
  }

  @Benchmark
  public PayloadsMaxPerfSubscriber requestChannelWithRequestAllStrategy(Blackhole blackhole)
      throws InterruptedException {
    PayloadsMaxPerfSubscriber subscriber = new PayloadsMaxPerfSubscriber(blackhole);
    client.requestChannel(PAYLOAD_FLUX).subscribe(subscriber);
    subscriber.await();

    return subscriber;
  }
}
