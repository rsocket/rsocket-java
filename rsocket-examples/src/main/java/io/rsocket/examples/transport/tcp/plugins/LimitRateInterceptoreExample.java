package io.rsocket.examples.transport.tcp.plugins;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.examples.transport.tcp.stream.StreamingClient;
import io.rsocket.plugins.RSocketInterceptor;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import io.rsocket.util.RSocketProxy;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.concurrent.Queues;

public class LimitRateInterceptoreExample {

  private static final Logger logger = LoggerFactory.getLogger(StreamingClient.class);

  public static void main(String[] args) {
    BlockingQueue<String> requests = new ArrayBlockingQueue<>(100);
    RSocketServer.create(
            (sp, sr) ->
                Mono.just(
                    new RSocket() {
                      @Override
                      public Flux<Payload> requestStream(Payload payload) {
                        return Flux.interval(Duration.ofMillis(100))
                            .doOnRequest(e -> requests.add("Responder requestN(" + e + ")"))
                            .map(aLong -> DefaultPayload.create("Interval: " + aLong));
                      }

                      @Override
                      public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
                        return Flux.from(payloads)
                            .doOnRequest(e -> requests.add("Responder requestN(" + e + ")"));
                      }
                    }))
        .interceptors(
            ir ->
                ir.forRequester((RSocketInterceptor) r -> new LimitRateRSocket(r, true))
                    .forResponder((RSocketInterceptor) r -> new LimitRateRSocket(r, false)))
        .bind(TcpServerTransport.create("localhost", 7000))
        .subscribe();

    RSocket socket =
        RSocketConnector.create()
            .interceptors(
                ir ->
                    ir.forRequester((RSocketInterceptor) r -> new LimitRateRSocket(r, true))
                        .forResponder((RSocketInterceptor) r -> new LimitRateRSocket(r, false)))
            .connect(TcpClientTransport.create("localhost", 7000))
            .block();

    socket
        .requestStream(DefaultPayload.create("Hello"))
        .doOnRequest(e -> requests.add("Requester requestN(" + e + ")"))
        .map(Payload::getDataUtf8)
        .doOnNext(logger::debug)
        .take(10)
        .then()
        .block();

    requests.forEach(request -> logger.debug("Requested : {}", request));
    requests.clear();

    logger.debug("-----------------------------------------------------------------");
    logger.debug("Does requestChannel");
    socket
        .requestChannel(
            Flux.<Payload, Long>generate(
                    () -> 1L,
                    (s, sink) -> {
                      sink.next(DefaultPayload.create("Next " + s));
                      return ++s;
                    })
                .doOnRequest(e -> requests.add("Requester Upstream requestN(" + e + ")")))
        .doOnRequest(e -> requests.add("Requester Downstream requestN(" + e + ")"))
        .map(Payload::getDataUtf8)
        .doOnNext(logger::debug)
        .take(10)
        .then()
        .doFinally(signalType -> socket.dispose())
        .then()
        .block();

    requests.forEach(request -> logger.debug("Requested : {}", request));
  }

  static class LimitRateRSocket extends RSocketProxy {

    private final boolean requesterSide;
    private final int highTide;
    private final int lowTide;

    public LimitRateRSocket(RSocket source, boolean requesterSide) {
      this(source, requesterSide, Queues.SMALL_BUFFER_SIZE);
    }

    public LimitRateRSocket(RSocket source, boolean requesterSide, int highTide) {
      this(source, requesterSide, highTide, highTide);
    }

    public LimitRateRSocket(RSocket source, boolean requesterSide, int highTide, int lowTide) {
      super(source);
      this.requesterSide = requesterSide;
      this.highTide = highTide;
      this.lowTide = lowTide;
    }

    @Override
    public Flux<Payload> requestStream(Payload payload) {
      Flux<Payload> flux = super.requestStream(payload);
      if (requesterSide) {
        return flux;
      }
      return flux.limitRate(highTide, lowTide);
    }

    @Override
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
      if (requesterSide) {
        return super.requestChannel(Flux.from(payloads).limitRate(highTide, lowTide));
      }
      return super.requestChannel(payloads).limitRate(highTide, lowTide);
    }
  }
}
