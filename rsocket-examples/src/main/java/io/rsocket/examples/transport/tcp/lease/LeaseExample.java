package io.rsocket.examples.transport.tcp.lease;

import static java.time.Duration.ofSeconds;

import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.lease.Lease;
import io.rsocket.lease.LeaseStats;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import java.util.Date;
import java.util.function.Consumer;
import java.util.function.Function;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class LeaseExample {
  private static final String SERVER_TAG = "server";
  public static final String CLIENT_TAG = "client";

  public static void main(String[] args) {

    LeaseSender serverLeaseSender = new LeaseSender(SERVER_TAG);
    LeaseReceiver serverLeaseReceiver = new LeaseReceiver(SERVER_TAG);
    CloseableChannel server =
        RSocketFactory.receive()
            .lease()
            .leaseSender(serverLeaseSender)
            .leaseReceiver(serverLeaseReceiver)
            .acceptor((setup, sendingRSocket) -> Mono.just(new ServerAcceptor(sendingRSocket)))
            .transport(TcpServerTransport.create("localhost", 7000))
            .start()
            .block();

    LeaseSender clientLeaseSender = new LeaseSender(CLIENT_TAG);
    LeaseReceiver clientLeaseReceiver = new LeaseReceiver(CLIENT_TAG);
    RSocket clientRSocket =
        RSocketFactory.connect()
            .lease()
            .leaseSender(clientLeaseSender)
            .leaseReceiver(clientLeaseReceiver)
            .acceptor(rSocket -> new ClientAcceptor())
            .transport(TcpClientTransport.create(server.address()))
            .start()
            .block();

    Flux.interval(ofSeconds(1))
        .flatMap(
            signal -> {
              System.out.println("Client requester availability: " + clientRSocket.availability());
              return clientRSocket
                  .requestResponse(DefaultPayload.create("Client request " + new Date()))
                  .doOnError(err -> System.out.println("Client request error: " + err))
                  .onErrorResume(err -> Mono.empty());
            })
        .subscribe(resp -> System.out.println("Client requester response: " + resp.getDataUtf8()));

    clientRSocket.onClose().block();
    server.dispose();
  }

  private static class LeaseSender implements Function<LeaseStats, Flux<Lease>> {
    private final String tag;

    public LeaseSender(String tag) {
      this.tag = tag;
    }

    @Override
    public Flux<Lease> apply(LeaseStats leaseStats) {
      return Flux.interval(ofSeconds(1), ofSeconds(10))
          .onBackpressureLatest()
          .map(
              tick -> {
                System.out.println(
                    String.format(
                        "%s responder sends new leaseSender, current: %s",
                        tag, leaseStats.lease().toString()));
                return Lease.create(3_000, 5);
              });
    }
  }

  private static class LeaseReceiver implements Consumer<Flux<Lease>> {
    private final String tag;

    public LeaseReceiver(String tag) {
      this.tag = tag;
    }

    @Override
    public void accept(Flux<Lease> receivedLeases) {
      receivedLeases.subscribe(
          l ->
              System.out.println(
                  String.format(
                      "%s received leaseSender - ttl: %d, requests: %d",
                      tag, l.getTimeToLiveMillis(), l.getAllowedRequests())));
    }
  }

  private static class ClientAcceptor extends AbstractRSocket {
    @Override
    public Mono<Payload> requestResponse(Payload payload) {
      return Mono.just(DefaultPayload.create("Client Response " + new Date()));
    }
  }

  private static class ServerAcceptor extends AbstractRSocket {
    private final RSocket senderRSocket;

    public ServerAcceptor(RSocket senderRSocket) {
      this.senderRSocket = senderRSocket;
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
      System.out.println("Server requester availability: " + senderRSocket.availability());
      senderRSocket
          .requestResponse(DefaultPayload.create("Server request " + new Date()))
          .doOnError(err -> System.out.println("Server request error: " + err))
          .onErrorResume(err -> Mono.empty())
          .subscribe(
              resp -> System.out.println("Server requester response: " + resp.getDataUtf8()));

      return Mono.just(DefaultPayload.create("Server Response " + new Date()));
    }
  }
}
