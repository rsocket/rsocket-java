/*
 * Copyright 2015-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.rsocket.examples.transport.tcp.loadbalancer;

import io.rsocket.RSocketClient;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketServer;
import io.rsocket.loadbalance.LoadbalanceRSocketClient;
import io.rsocket.loadbalance.LoadbalanceTarget;
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

    LoadbalanceTarget target8080 = LoadbalanceTarget.from("8080", TcpClientTransport.create(8080));
    LoadbalanceTarget target8081 = LoadbalanceTarget.from("8081", TcpClientTransport.create(8081));
    LoadbalanceTarget target8082 = LoadbalanceTarget.from("8082", TcpClientTransport.create(8082));

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
                      return Collections.singletonList(target8080);
                    case 2:
                      return Arrays.asList(target8080, target8081);
                    case 3:
                      return Arrays.asList(target8080, target8082);
                    case 4:
                      return Arrays.asList(target8081, target8082);
                    case 5:
                      return Arrays.asList(target8080, target8081, target8082);
                    case 6:
                      return Collections.emptyList();
                    case 7:
                      return Collections.emptyList();
                    default:
                      return Arrays.asList(target8080, target8081, target8082);
                  }
                });

    RSocketClient rSocketClient =
        LoadbalanceRSocketClient.builder(producer).roundRobinLoadbalanceStrategy().build();

    for (int i = 0; i < 10000; i++) {
      try {

        rSocketClient.requestResponse(Mono.just(DefaultPayload.create("test" + i))).block();
      } catch (Throwable t) {
        // no ops
      }
    }
  }
}
