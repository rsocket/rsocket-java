package io.rsocket.examples.transport.tcp.loadbalancer;

import io.rsocket.RSocketClient;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.loadbalance.LoadBalancedRSocketClient;
import io.rsocket.loadbalance.LoadbalanceTarget;
import io.rsocket.loadbalance.WeightedLoadbalanceStrategy;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RoundRobinRSocketLoadbalancerExample {

  public static void main(String[] args) {
    CloseableChannel server1 =
        RSocketServer.create(
                SocketAcceptor.forRequestResponse(
                    p -> {
                      System.out.println("Server 1 got fnf " + p.getDataUtf8());
                      return Mono.just(DefaultPayload.create("Server 1 response"))
                          .delayElement(Duration.ofMillis(100));
                    }))
            .bindNow(TcpServerTransport.create(8080));

    CloseableChannel server2 =
        RSocketServer.create(
                SocketAcceptor.forRequestResponse(
                    p -> {
                      System.out.println("Server 2 got fnf " + p.getDataUtf8());
                      return Mono.just(DefaultPayload.create("Server 2 response"))
                          .delayElement(Duration.ofMillis(100));
                    }))
            .bindNow(TcpServerTransport.create(8081));

    CloseableChannel server3 =
        RSocketServer.create(
                SocketAcceptor.forRequestResponse(
                    p -> {
                      System.out.println("Server 3 got fnf " + p.getDataUtf8());
                      return Mono.just(DefaultPayload.create("Server 3 response"))
                          .delayElement(Duration.ofMillis(100));
                    }))
            .bindNow(TcpServerTransport.create(8082));

    Flux<List<LoadbalanceTarget>> producer =
        Flux.interval(Duration.ofSeconds(5))
            .log()
            .map(
                i -> {
                  int val = i.intValue();
                  switch (val) {
                    case 0:
                      return Collections.emptyList();
                    case 1:
                      return Collections.singletonList(
                          new LoadbalanceTarget(
                              "8080",
                              RSocketConnector.connectWith(TcpClientTransport.create(8080))));
                    case 2:
                      return Arrays.asList(
                          new LoadbalanceTarget(
                              "8080",
                              RSocketConnector.connectWith(TcpClientTransport.create(8080))),
                          new LoadbalanceTarget(
                              "8081",
                              RSocketConnector.connectWith(TcpClientTransport.create(8081))));
                    case 3:
                      return Arrays.asList(
                          new LoadbalanceTarget(
                              "8080",
                              RSocketConnector.connectWith(TcpClientTransport.create(8080))),
                          new LoadbalanceTarget(
                              "8082",
                              RSocketConnector.connectWith(TcpClientTransport.create(8082))));
                    case 4:
                      return Arrays.asList(
                          new LoadbalanceTarget(
                              "8081",
                              RSocketConnector.connectWith(TcpClientTransport.create(8081))),
                          new LoadbalanceTarget(
                              "8082",
                              RSocketConnector.connectWith(TcpClientTransport.create(8082))));
                    case 5:
                      return Arrays.asList(
                          new LoadbalanceTarget(
                              "8080",
                              RSocketConnector.connectWith(TcpClientTransport.create(8080))),
                          new LoadbalanceTarget(
                              "8081",
                              RSocketConnector.connectWith(TcpClientTransport.create(8081))),
                          new LoadbalanceTarget(
                              "8082",
                              RSocketConnector.connectWith(TcpClientTransport.create(8082))));
                    case 6:
                      return Collections.emptyList();
                    case 7:
                      return Collections.emptyList();
                    default:
                    case 8:
                      return Arrays.asList(
                          new LoadbalanceTarget(
                              "8080",
                              RSocketConnector.connectWith(TcpClientTransport.create(8080))),
                          new LoadbalanceTarget(
                              "8081",
                              RSocketConnector.connectWith(TcpClientTransport.create(8081))),
                          new LoadbalanceTarget(
                              "8082",
                              RSocketConnector.connectWith(TcpClientTransport.create(8082))));
                  }
                });

    RSocketClient loadBalancedRSocketClient =
        LoadBalancedRSocketClient.create(new WeightedLoadbalanceStrategy(), producer);

    for (int i = 0; i < 10000; i++) {
      loadBalancedRSocketClient
          .requestResponse(Mono.just(DefaultPayload.create("test" + i)))
          .block();
    }
  }
}
