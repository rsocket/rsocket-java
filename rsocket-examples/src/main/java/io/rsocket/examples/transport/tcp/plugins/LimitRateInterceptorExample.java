package io.rsocket.examples.transport.tcp.plugins;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.plugins.LimitRateInterceptor;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import java.time.Duration;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

public class LimitRateInterceptorExample {

  private static final Logger logger = LoggerFactory.getLogger(LimitRateInterceptorExample.class);

  public static void main(String[] args) {
    RSocketServer.create(
            SocketAcceptor.with(
                new RSocket() {
                  @Override
                  public Flux<Payload> requestStream(Payload payload) {
                    return Flux.interval(Duration.ofMillis(100))
                        .doOnRequest(
                            e -> logger.debug("Server publisher receives request for " + e))
                        .map(aLong -> DefaultPayload.create("Interval: " + aLong));
                  }

                  @Override
                  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
                    return Flux.from(payloads)
                        .doOnRequest(
                            e -> logger.debug("Server publisher receives request for " + e));
                  }
                }))
        .interceptors(registry -> registry.forResponder(LimitRateInterceptor.forResponder(64)))
        .bind(TcpServerTransport.create("localhost", 7000))
        .subscribe();

    RSocket socket =
        RSocketConnector.create()
            .interceptors(registry -> registry.forRequester(LimitRateInterceptor.forRequester(64)))
            .connect(TcpClientTransport.create("localhost", 7000))
            .block();

    logger.debug(
        "\n\nStart of requestStream interaction\n" + "----------------------------------\n");

    socket
        .requestStream(DefaultPayload.create("Hello"))
        .doOnRequest(e -> logger.debug("Client sends requestN(" + e + ")"))
        .map(Payload::getDataUtf8)
        .doOnNext(logger::debug)
        .take(10)
        .then()
        .block();

    logger.debug(
        "\n\nStart of requestChannel interaction\n" + "-----------------------------------\n");

    socket
        .requestChannel(
            Flux.<Payload, Long>generate(
                    () -> 1L,
                    (s, sink) -> {
                      sink.next(DefaultPayload.create("Next " + s));
                      return ++s;
                    })
                .doOnRequest(e -> logger.debug("Client publisher receives request for " + e)))
        .doOnRequest(e -> logger.debug("Client sends requestN(" + e + ")"))
        .map(Payload::getDataUtf8)
        .doOnNext(logger::debug)
        .take(10)
        .then()
        .doFinally(signalType -> socket.dispose())
        .then()
        .block();
  }
}
