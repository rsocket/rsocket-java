package io.rsocket.examples.transport.tcp.lease;

import static java.time.Duration.*;

import io.rsocket.*;
import io.rsocket.lease.LeaseControl;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.NettyContextCloseable;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.PayloadImpl;
import java.util.Date;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

public class LeaseClientServeReqRep {
  private static final Logger LOGGER = LoggerFactory.getLogger("io.rsocket.examples.lease_req_rep");

  public static void main(String[] args) {

    LeaseControlSource serverLeaseControl = new LeaseControlSource();
    NettyContextCloseable nettyContextCloseable =
        RSocketFactory.receive()
            .enableLease(serverLeaseControl)
            .acceptor(
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

    LeaseControlSource clientLeaseControl = new LeaseControlSource();
    RSocket clientSocket =
        RSocketFactory.connect()
            .enableLease(clientLeaseControl)
            .transport(TcpClientTransport.create("localhost", 7000))
            .start()
            .block();

    Flux.interval(ofSeconds(1))
        .flatMap(
            signal -> {
              LOGGER.info("Availability: " + clientSocket.availability());
              return clientSocket
                  .requestResponse(new PayloadImpl("Client request " + new Date()))
                  .onErrorResume(
                      err ->
                          Mono.<Payload>empty().doOnTerminate(() -> LOGGER.info("Error: " + err)));
            })
        .subscribe(resp -> LOGGER.info("Client response: " + resp.getDataUtf8()));
    serverLeaseControl
        .leaseControl()
        .flatMapMany(lc -> Flux.interval(ofSeconds(1), ofSeconds(10)).map(signal -> lc))
        .flatMapIterable(LeaseControl::getLeaseRSockets)
        .subscribe(ref -> ref.grantLease(7, 5_000));

    clientSocket.onClose().block();
  }

  private static class LeaseControlSource implements Consumer<LeaseControl> {
    private final MonoProcessor<LeaseControl> leaseControlMono = MonoProcessor.create();

    public Mono<LeaseControl> leaseControl() {
      return leaseControlMono;
    }

    @Override
    public void accept(LeaseControl leaseControl) {
      leaseControlMono.onNext(leaseControl);
    }
  }
}
