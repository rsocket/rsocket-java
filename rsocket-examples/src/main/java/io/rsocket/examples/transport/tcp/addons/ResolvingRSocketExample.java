package io.rsocket.examples.transport.tcp.addons;

import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.addons.ResolvingRSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.examples.transport.tcp.channel.ChannelEchoClient;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import java.time.Duration;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

public class ResolvingRSocketExample {

  private static final Logger logger = LoggerFactory.getLogger(ChannelEchoClient.class);

  public static void main(String[] args) {
    SocketAcceptor echoAcceptor =
        new SocketAcceptor() {
          @Override
          public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
            return Mono.just(
                new RSocket() {
                  @Override
                  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
                    return Flux.from(payloads)
                        .map(Payload::getDataUtf8)
                        .map(s -> "Echo: " + s)
                        .map(DefaultPayload::create);
                  }
                });
          }
        };

    CloseableChannel channel =
        RSocketServer.create(echoAcceptor).bindNow(TcpServerTransport.create("localhost", 7000));

    RSocket socket =
        RSocketConnector.connectWith(TcpClientTransport.create("localhost", 7000))
            .as(ResolvingRSocket::new);

    Flux.defer(
            () ->
                socket.requestChannel(
                    Flux.interval(Duration.ofMillis(1000))
                        .map(i -> DefaultPayload.create("Hello"))))
        .map(Payload::getDataUtf8)
        .doOnNext(logger::debug)
        .then()
        .retryWhen(Retry.backoff(100, Duration.ofMillis(100)))
        .subscribe();

    logger.info("Disposing Server");
    channel.dispose();

    RSocketServer.create(echoAcceptor).bindNow(TcpServerTransport.create("localhost", 7000));

    logger.info("Server started");
  }
}
