package io.rsocket.core;

import io.rsocket.Closeable;
import io.rsocket.Payload;
import io.rsocket.PayloadsPerfSubscriber;
import io.rsocket.RSocket;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.local.LocalClientTransport;
import io.rsocket.transport.local.LocalServerTransport;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.transport.netty.server.WebsocketServerTransport;
import io.rsocket.util.ByteBufPayload;
import io.rsocket.util.EmptyPayload;
import java.lang.reflect.Field;
import java.util.Queue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 10, time = 10)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class RSocketPerf {

  @Param({"tcp", /*"websocket", "local", */ "shm"})
  String transportType;

  @Param({
    /*"0", "64", */
    "14024" /*, "131072", "1048576", "15728640"*/
  })
  String payloadSize;

  Payload payload;
  Mono<Payload> payloadMono;
  Flux<Payload> payloadsFlux;

  RSocket client;
  Closeable server;
  Queue clientsQueue;

  @TearDown
  public void tearDown() {
    client.dispose();
    server.dispose();
    payload.release();
  }

  @TearDown(Level.Iteration)
  public void awaitToBeConsumed() {
    while (!clientsQueue.isEmpty()) {
      LockSupport.parkNanos(1000);
    }
  }

  @Setup
  public void setUp() throws NoSuchFieldException, IllegalAccessException, ClassNotFoundException {
    ClientTransport clientTransport;
    ServerTransport<?> serverTransport;
    switch (transportType) {
      case "tcp":
        clientTransport = TcpClientTransport.create(8081);
        serverTransport = TcpServerTransport.create(8081);
        break;
      case "websocket":
        clientTransport = WebsocketClientTransport.create(8081);
        serverTransport = WebsocketServerTransport.create(8081);
        break;
      case "local":
      default:
        clientTransport = LocalClientTransport.create("server");
        serverTransport = LocalServerTransport.create("server");
        break;
    }
    Payload payload;
    int payloadSize = Integer.parseInt(this.payloadSize);
    if (payloadSize == 0) {
      payload = EmptyPayload.INSTANCE;
    } else {
      byte[] randomMetadata = new byte[payloadSize / 2];
      byte[] randomData = new byte[payloadSize / 2];
      ThreadLocalRandom.current().nextBytes(randomData);
      ThreadLocalRandom.current().nextBytes(randomMetadata);

      payload = ByteBufPayload.create(randomData, randomMetadata);
    }

    this.payload = payload;
    this.payloadMono = Mono.fromSupplier(payload::retain);
    this.payloadsFlux = Flux.range(0, 100000).map(__ -> payload.retain());
    this.server =
        RSocketServer.create(
                (setup, sendingSocket) ->
                    Mono.just(
                        new RSocket() {

                          @Override
                          public Mono<Void> fireAndForget(Payload payload) {
                            payload.release();
                            return Mono.empty();
                          }

                          @Override
                          public Mono<Payload> requestResponse(Payload payload) {
                            payload.release();
                            return payloadMono;
                          }

                          @Override
                          public Flux<Payload> requestStream(Payload payload) {
                            payload.release();
                            return payloadsFlux;
                          }

                          @Override
                          public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
                            return Flux.from(payloads);
                          }
                        }))
            .payloadDecoder(PayloadDecoder.ZERO_COPY)
            .bind(serverTransport)
            .block();

    this.client =
        RSocketConnector.create()
            .payloadDecoder(PayloadDecoder.ZERO_COPY)
            .connect(clientTransport)
            .block();

    try {
      Field sendProcessorField = RSocketRequester.class.getDeclaredField("sendProcessor");
      sendProcessorField.setAccessible(true);

      clientsQueue = (Queue) sendProcessorField.get(client);
    } catch (Throwable t) {
      Field sendProcessorField =
          Class.forName("io.rsocket.core.RequesterResponderSupport")
              .getDeclaredField("sendProcessor");
      sendProcessorField.setAccessible(true);

      clientsQueue = (Queue) sendProcessorField.get(client);
    }
  }

  //  @Benchmark
  //  @SuppressWarnings("unchecked")
  //  public PayloadsPerfSubscriber fireAndForget(Blackhole blackhole) throws InterruptedException {
  //    PayloadsPerfSubscriber subscriber = new PayloadsPerfSubscriber(blackhole);
  //    client.fireAndForget(payload.retain()).subscribe((CoreSubscriber) subscriber);
  //    subscriber.await();
  //
  //    return subscriber;
  //  }

  @Benchmark
  public PayloadsPerfSubscriber requestResponse(Blackhole blackhole) throws InterruptedException {
    PayloadsPerfSubscriber subscriber = new PayloadsPerfSubscriber(blackhole);
    client.requestResponse(payload.retain()).subscribe(subscriber);
    subscriber.await();

    return subscriber;
  }
  //
  //  @Benchmark
  //  public PayloadsPerfSubscriber requestStreamWithRequestByOneStrategy(Blackhole blackhole)
  //      throws InterruptedException {
  //    PayloadsPerfSubscriber subscriber = new PayloadsPerfSubscriber(blackhole);
  //    client.requestStream(payload.retain()).subscribe(subscriber);
  //    subscriber.await();
  //
  //    return subscriber;
  //  }
  //
  //  @Benchmark
  //  public PayloadsMaxPerfSubscriber requestStreamWithRequestAllStrategy(Blackhole blackhole)
  //      throws InterruptedException {
  //    PayloadsMaxPerfSubscriber subscriber = new PayloadsMaxPerfSubscriber(blackhole);
  //    client.requestStream(payload.retain()).subscribe(subscriber);
  //    subscriber.await();
  //
  //    return subscriber;
  //  }
  //
  //  @Benchmark
  //  public PayloadsPerfSubscriber requestChannelWithRequestByOneStrategy(Blackhole blackhole)
  //      throws InterruptedException {
  //    PayloadsPerfSubscriber subscriber = new PayloadsPerfSubscriber(blackhole);
  //    client.requestChannel(payloadsFlux).subscribe(subscriber);
  //    subscriber.await();
  //
  //    return subscriber;
  //  }
  //
  //  @Benchmark
  //  public PayloadsMaxPerfSubscriber requestChannelWithRequestAllStrategy(Blackhole blackhole)
  //      throws InterruptedException {
  //    PayloadsMaxPerfSubscriber subscriber = new PayloadsMaxPerfSubscriber(blackhole);
  //    client.requestChannel(payloadsFlux).subscribe(subscriber);
  //    subscriber.await();
  //
  //    return subscriber;
  //  }
}
