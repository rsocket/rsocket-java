package io.rsocket.examples.transport.tcp.lease;

import static java.time.Duration.ofSeconds;

import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.lease.LeaseOptions;
import io.rsocket.lease.Leases;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import java.time.Duration;
import java.util.Date;
import java.util.function.BiConsumer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

public class LeaseExample {

  public static void main(String[] args) {

    LeaseSenderConsumer serverLeaseSenderConsumer = new LeaseSenderConsumer();
    CloseableChannel server =
        RSocketFactory.receive()
            .lease(serverLeaseSenderConsumer)
            .acceptor((setup, sendingRSocket) -> Mono.just(new ServerAcceptor(sendingRSocket)))
            .transport(TcpServerTransport.create("localhost", 7000))
            .start()
            .block();

    LeaseSenderConsumer clientLeaseSenderConsumer = new LeaseSenderConsumer();
    RSocket clientRSocket =
        RSocketFactory.connect()
            .lease(clientLeaseSenderConsumer)
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

    clientLeaseSenderConsumer
        .leaseSender()
        .flatMapMany(
            sender -> {
              sender.receiveLease(
                  (ttl, requests, metadata) ->
                      System.out.println(
                          String.format(
                              "Client received lease - ttl: %s, requests: %s", ttl, requests)));
              return Flux.interval(ofSeconds(1), ofSeconds(10))
                  .map(tick -> sender)
                  .onBackpressureLatest();
            })
        .subscribe(
            sender -> {
              System.out.println(
                  "Client responder sends new lease, current: " + sender.responderLease());
              sender.sendLease(3_000, 5);
              System.out.println("Client current requester lease: " + sender.requesterLease());
            });

    serverLeaseSenderConsumer
        .leaseSender()
        .flatMapMany(
            sender -> {
              sender.receiveLease(
                  (ttl, requests, metadata) ->
                      System.out.println(
                          String.format(
                              "Server received lease - ttl: %s, requests: %s", ttl, requests)));
              return Flux.interval(ofSeconds(1), ofSeconds(10))
                  .map(tick -> sender)
                  .onBackpressureLatest();
            })
        .take(Duration.ofSeconds(120))
        .doFinally(s -> clientRSocket.dispose())
        .subscribe(
            sender -> {
              System.out.println(
                  "Server responder sends new lease, current: " + sender.responderLease());
              sender.sendLease(7_000, 5);
              System.out.println("Server current requester lease: " + sender.requesterLease());
            });

    clientRSocket.onClose().block();
    server.dispose();
  }

  private static class LeaseSenderConsumer implements BiConsumer<Leases, LeaseOptions> {
    private final MonoProcessor<Leases> leaseSenderMono = MonoProcessor.create();

    public Mono<Leases> leaseSender() {
      return leaseSenderMono;
    }

    @Override
    public void accept(Leases leases, LeaseOptions leaseOptions) {
      leaseSenderMono.onNext(leases);
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
