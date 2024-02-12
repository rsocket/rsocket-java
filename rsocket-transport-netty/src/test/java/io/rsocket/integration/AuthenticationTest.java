package io.rsocket.integration;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.exceptions.RejectedSetupException;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.netty.tcp.TcpClient;
import reactor.netty.tcp.TcpServer;
import reactor.test.StepVerifier;

public class AuthenticationTest {

  private static final Logger LOG = LoggerFactory.getLogger(AuthenticationTest.class);
  private static final int PORT = 23200;

  @Test
  void authTest() {
    Hooks.onOperatorDebug();
    createServer().block();
    RSocket rsocketClient = createClient().block();

    StepVerifier.create(rsocketClient.requestResponse(DefaultPayload.create("Client: Hello")))
        .expectError(RejectedSetupException.class)
        .verify();
  }

  private static Mono<CloseableChannel> createServer() {
    LOG.info("Starting server at port {}", PORT);
    RSocketServer rSocketServer =
        RSocketServer.create((connectionSetupPayload, rSocket) -> Mono.just(new MyServerRsocket()));

    TcpServer tcpServer = TcpServer.create().host("localhost").port(PORT);

    return rSocketServer
        .interceptors(
            interceptorRegistry ->
                interceptorRegistry.forSocketAcceptor(
                    socketAcceptor ->
                        (setup, sendingSocket) -> {
                          if (true) { // TODO here would be an authentication check based on the
                            // setup payload
                            return Mono.error(new RejectedSetupException("ACCESS_DENIED"));
                          } else {
                            return socketAcceptor.accept(setup, sendingSocket);
                          }
                        }))
        .bind(TcpServerTransport.create(tcpServer))
        .doOnNext(closeableChannel -> LOG.info("RSocket server started."));
  }

  private static Mono<RSocket> createClient() {
    LOG.info("Connecting....");
    return RSocketConnector.create()
        .connect(TcpClientTransport.create(TcpClient.create().host("localhost").port(PORT)))
        .doOnNext(rSocket -> LOG.info("Successfully connected to server"))
        .doOnError(throwable -> LOG.error("Failed to connect to server"));
  }

  public static class MyServerRsocket implements RSocket {
    private static final Logger LOG = LoggerFactory.getLogger(MyServerRsocket.class);

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
      LOG.info("Got a request with payload: {}", payload.getDataUtf8());
      return Mono.just("Response data blah blah blah").map(DefaultPayload::create);
    }
  }
}
