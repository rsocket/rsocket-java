package io.rsocket.examples.transport.tcp.lease;

import io.rsocket.*;
import io.rsocket.lease.Lease;
import io.rsocket.lease.LeaseControl;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.NettyContextCloseable;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.PayloadImpl;
import java.time.Duration;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.*;

public class LeaseClientServerDelayedBidi {
  private static final Logger LOGGER =
      LoggerFactory.getLogger("io.rsocket.examples.lease_delayed_bidi");

  public static void main(String[] args) {
    LeaseClosable<NettyContextCloseable> serverHandle =
        RSocketFactory.receive()
            .enableLease()
            .leaseAcceptor(
                (setup, reactiveSocket) -> {
                  reactiveSocket
                      .leaseControl()
                      .map(LeaseControl::getLeases)
                      .get()
                      .filter(Lease::isValid)
                      .next()
                      .flatMapMany(
                          l ->
                              reactiveSocket
                                  .requestStream(new PayloadImpl("Hello-Bidi"))
                                  .map(Payload::getDataUtf8)
                                  .log())
                      .subscribe();

                  return Mono.just(new AbstractRSocket() {});
                })
            .transport(TcpServerTransport.create("localhost", 7000))
            .start()
            .block();

    serverHandle
        .getLeaseControl()
        .flatMapMany(
            leaseControl ->
                leaseControl
                    .map(LeaseControl::getLeases)
                    .orElseGet(
                        () -> Flux.error(new IllegalStateException("Server: no leases support"))))
        .subscribe(
            lease -> LOGGER.info("Server got lease: " + lease),
            err -> LOGGER.info("Server receive lease error: " + err));

    LeaseRSocket clientSocket =
        RSocketFactory.connect()
            .enableLease()
            .leaseAcceptor(
                rSocket ->
                    new AbstractRSocket() {
                      @Override
                      public Flux<Payload> requestStream(Payload payload) {
                        return Flux.interval(Duration.ofSeconds(1))
                            .map(aLong -> new PayloadImpl("Bi-di Response => " + aLong));
                      }
                    })
            .transport(TcpClientTransport.create("localhost", 7000))
            .start()
            .block();
    Optional<LeaseControl> clientLeaseControl = clientSocket.leaseControl();
    clientLeaseControl.ifPresent(
        lc ->
            Mono.delay(Duration.ofSeconds(5))
                .subscribe(signal -> lc.grantLease(1, Duration.ofMinutes(1).toMillis())));
    clientLeaseControl
        .map(LeaseControl::getLeases)
        .orElse(Flux.error(new IllegalStateException("Client: no leases support")))
        .subscribe(
            lease -> LOGGER.info("Client got lease: " + lease),
            err -> LOGGER.info("Client receive lease error: " + err));
    Optional<LeaseControl> serverLeaseControl = serverHandle.getLeaseControl().block();
    serverLeaseControl.ifPresent(lc -> lc.grantLease(2, Duration.ofMinutes(1).toMillis()));

    clientSocket.onClose().block();
  }
}
