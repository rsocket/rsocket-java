package io.rsocket.examples.transport.tcp.lease.advanced.multiclient;

import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.examples.transport.tcp.lease.advanced.common.DefaultDeferringLeaseReceiver;
import io.rsocket.examples.transport.tcp.lease.advanced.common.LeaseWaitingRSocket;
import io.rsocket.lease.Leases;
import io.rsocket.plugins.RSocketInterceptor;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.ByteBufPayload;
import java.util.Objects;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

public class RequestingClient {
  private static final Logger logger = LoggerFactory.getLogger(RequestingClient.class);

  public static void main(String[] args) {
    DefaultDeferringLeaseReceiver leaseReceiver =
        new DefaultDeferringLeaseReceiver(UUID.randomUUID().toString());

    RSocket clientRSocket =
        RSocketConnector.create()
            .lease(() -> Leases.create().receiver(leaseReceiver))
            .interceptors(
                ir ->
                    ir.forRequester(
                        (RSocketInterceptor) r -> new LeaseWaitingRSocket(r, leaseReceiver)))
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
