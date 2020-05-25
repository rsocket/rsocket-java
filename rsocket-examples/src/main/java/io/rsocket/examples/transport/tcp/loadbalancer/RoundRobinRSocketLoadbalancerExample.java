package io.rsocket.examples.transport.tcp.loadbalancer;

import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.addons.LoadBalancedRSocket;
import io.rsocket.addons.ResolvingRSocket;
import io.rsocket.addons.RoundRobinRSocketPool;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import java.util.List;
import java.util.stream.Collectors;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RoundRobinRSocketLoadbalancerExample {

  public static void main(String[] args) {
    CloseableChannel server1 =
        RSocketServer.create(
                SocketAcceptor.forRequestResponse(
                    p -> {
                      System.out.println("Server 1 got fnf " + p.getDataUtf8());
                      return Mono.just(DefaultPayload.create("Server 1 response"));
                    }))
            .bindNow(TcpServerTransport.create(8080));

    CloseableChannel server2 =
        RSocketServer.create(
                SocketAcceptor.forRequestResponse(
                    p -> {
                      System.out.println("Server 2 got fnf " + p.getDataUtf8());
                      return Mono.just(DefaultPayload.create("Server 2 response"));
                    }))
            .bindNow(TcpServerTransport.create(8081));

    Mono<List<RSocket>> collect =
        Flux.just(
                new ResolvingRSocket(RSocketConnector.connectWith(TcpClientTransport.create(8080))),
                new ResolvingRSocket(RSocketConnector.connectWith(TcpClientTransport.create(8081))))
            .collect(Collectors.toList());
    LoadBalancedRSocket loadBalancedRSocket =
        new LoadBalancedRSocket(new RoundRobinRSocketPool(collect));

    loadBalancedRSocket.requestResponse(DefaultPayload.create("test1")).block();
    loadBalancedRSocket.requestResponse(DefaultPayload.create("test2")).block();
    loadBalancedRSocket.requestResponse(DefaultPayload.create("test3")).block();
  }
}
