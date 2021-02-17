package io.rsocket.examples.transport.tcp.lease.advanced.multiclient;

import io.rsocket.RSocket;
import io.rsocket.core.LeaseConfig;
import io.rsocket.core.RSocketConnector;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.ByteBufPayload;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

public class RequestingClient {
  private static final Logger logger = LoggerFactory.getLogger(RequestingClient.class);

  public static void main(String[] args) {

    RSocket clientRSocket =
        RSocketConnector.create()
            .lease(LeaseConfig::deferOnNoLease)
            .connect(TcpClientTransport.create("localhost", 7000))
            .block();

    Objects.requireNonNull(clientRSocket);

    // generate stream of fnfs
    Flux.generate(
            () -> 0L,
            (state, sink) -> {
              sink.next(state);
              return state + 1;
            })
        .concatMap(
            tick -> {
              logger.info("Requesting FireAndForget({})", tick);
              return clientRSocket.fireAndForget(ByteBufPayload.create("" + tick));
            })
        .blockLast();

    clientRSocket.onClose().block();
  }
}
