package io.rsocket.examples.transport.tcp.lease;

import io.rsocket.*;
import io.rsocket.lease.Lease;
import io.rsocket.lease.LeaseControl;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.NettyContextCloseable;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.PayloadImpl;
import java.time.Duration;
import java.util.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class LeaseClientServeReqRep {
  private static final Logger LOGGER = LoggerFactory.getLogger("io.rsocket.examples.lease_req_rep");

  public static void main(String[] args) {
    LeaseClosable<NettyContextCloseable> serverHandle =
        RSocketFactory.receive()
            .leaseAcceptor(() ->
                (setup, reactiveSocket) ->
                    Mono.just(
                        new AbstractRSocket() {
                          @Override
                          public Mono<Payload> requestResponse(Payload payload) {
                            return Mono.just(new PayloadImpl("Server Response " + new Date()));
                          }
                        }))
            .transport(TcpServerTransport.create("localhost", 7000))
            .start()
            .block();

    LeaseRSocket clientSocket =
        RSocketFactory.connect()
            .emptyLeaseAcceptor()
            .transport(TcpClientTransport.create("localhost", 7000))
            .start()
            .block();
    LeaseControl serverLeaseControl =
        serverHandle
            .getLeaseControl()
            .block()
            .orElseThrow(() -> new IllegalStateException("Lease control not available"));
    Flux.interval(Duration.ofSeconds(1), Duration.ofSeconds(10))
        .subscribe(
            signal -> {
              serverLeaseControl.grantLease(3, Duration.ofSeconds(5).toMillis());
            });
    Flux.interval(Duration.ofSeconds(1))
            .flatMap(signal ->
                    clientSocket.requestResponse(new PayloadImpl("Client request " + new Date()))
                            .onErrorResume(err -> Mono.<Payload>empty().doOnTerminate(() -> LOGGER.info("Error: "+err))))
        .subscribe(resp -> LOGGER.info("Client response: " + resp.getDataUtf8()));

    clientSocket.onClose().block();
  }
}
