package io.rsocket.examples.transport.tcp.plugins;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
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
import reactor.util.concurrent.Queues;

public class LimitRateInterceptorExample {

  private static final Logger logger = LoggerFactory.getLogger(StreamingClient.class);

  public static void main(String[] args) {
    BlockingQueue<String> requests = new ArrayBlockingQueue<>(100);
    RSocketServer.create(
            SocketAcceptor.with(
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
                ir.forRequester(LimitRateInterceptor.forRequester())
                    .forResponder(LimitRateInterceptor.forResponder()))
        .bind(TcpServerTransport.create("localhost", 7000))
        .subscribe();

    RSocket socket =
        RSocketConnector.create()
            .interceptors(
                ir ->
                    ir.forRequester(LimitRateInterceptor.forRequester())
                        .forResponder(LimitRateInterceptor.forResponder()))
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

  static class LimitRateInterceptor implements RSocketInterceptor {

    final boolean requesterSide;
    final int highTide;
    final int lowTide;

    LimitRateInterceptor(boolean requesterSide, int highTide, int lowTide) {
      this.requesterSide = requesterSide;
      this.highTide = highTide;
      this.lowTide = lowTide;
    }

    @Override
    public RSocket apply(RSocket socket) {
      return new LimitRateRSocket(socket, requesterSide, highTide, lowTide);
    }

    public static LimitRateInterceptor forRequester() {
      return forRequester(Queues.SMALL_BUFFER_SIZE);
    }

    public static LimitRateInterceptor forRequester(int limit) {
      return forRequester(limit, limit);
    }

    public static LimitRateInterceptor forRequester(int highTide, int lowTide) {
      return new LimitRateInterceptor(true, highTide, lowTide);
    }

    public static LimitRateInterceptor forResponder() {
      return forRequester(Queues.SMALL_BUFFER_SIZE);
    }

    public static LimitRateInterceptor forResponder(int limit) {
      return forRequester(limit, limit);
    }

    public static LimitRateInterceptor forResponder(int highTide, int lowTide) {
      return new LimitRateInterceptor(false, highTide, lowTide);
    }
  }

  static class LimitRateRSocket extends RSocketProxy {

    final boolean requesterSide;
    final int highTide;
    final int lowTide;

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
