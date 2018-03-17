/*
 * Copyright 2015-2018 the original author or authors.
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

package io.rsocket.examples.transport.tcp.duplex;

import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import java.time.Duration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public final class DuplexClient {

  public static void main(String[] args) {
    RSocketFactory.receive()
        .acceptor(
            (setup, reactiveSocket) -> {
              reactiveSocket
                  .requestStream(DefaultPayload.create("Hello-Bidi"))
                  .map(Payload::getDataUtf8)
                  .log()
                  .subscribe();

              return Mono.just(new AbstractRSocket() {});
            })
        .transport(TcpServerTransport.create("localhost", 7000))
        .start()
        .subscribe();

    RSocket socket =
        RSocketFactory.connect()
            .acceptor(
                rSocket ->
                    new AbstractRSocket() {
                      @Override
                      public Flux<Payload> requestStream(Payload payload) {
                        return Flux.interval(Duration.ofSeconds(1))
                            .map(aLong -> DefaultPayload.create("Bi-di Response => " + aLong));
                      }
                    })
            .transport(TcpClientTransport.create("localhost", 7000))
            .start()
            .block();

    socket.onClose().block();
  }
}
