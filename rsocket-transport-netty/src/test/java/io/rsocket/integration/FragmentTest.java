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

package io.rsocket.integration;

import static org.assertj.core.api.Assertions.assertThat;

import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import io.rsocket.util.RSocketProxy;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class FragmentTest {
  private static final int frameSize = 64;
  private AbstractRSocket handler;
  private CloseableChannel server;
  private String message = null;
  private String metaData = null;
  private String responseMessage = null;

  @BeforeEach
  public void startup() {
    int randomPort = ThreadLocalRandom.current().nextInt(10_000, 20_000);
    StringBuilder message = new StringBuilder();
    StringBuilder responseMessage = new StringBuilder();
    StringBuilder metaData = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      message.append("REQUEST ");
      responseMessage.append("RESPONSE ");
      metaData.append("METADATA ");
    }
    this.message = message.toString();
    this.responseMessage = responseMessage.toString();
    this.metaData = metaData.toString();

    TcpServerTransport serverTransport = TcpServerTransport.create("localhost", randomPort);
    server =
        RSocketFactory.receive()
            .fragment(frameSize)
            .acceptor((setup, sendingSocket) -> Mono.just(new RSocketProxy(handler)))
            .transport(serverTransport)
            .start()
            .block();
  }

  private RSocket buildClient() {
    return RSocketFactory.connect()
        .fragment(frameSize)
        .transport(TcpClientTransport.create(server.address()))
        .start()
        .block();
  }

  @AfterEach
  public void cleanup() {
    server.dispose();
  }

  @Test
  void testFragmentNoMetaData() {
    System.out.println(
        "-------------------------------------------------testFragmentNoMetaData-------------------------------------------------");
    handler =
        new AbstractRSocket() {
          @Override
          public Flux<Payload> requestStream(Payload payload) {
            String request = payload.getDataUtf8();
            String metaData = payload.getMetadataUtf8();
            System.out.println("request message:   " + request);
            System.out.println("request metadata:  " + metaData);

            return Flux.just(DefaultPayload.create(responseMessage));
          }
        };

    RSocket client = buildClient();

    System.out.println("original message:  " + message);
    System.out.println("original metadata: " + metaData);
    Payload payload = client.requestStream(DefaultPayload.create(message)).blockLast();
    System.out.println("response message:  " + payload.getDataUtf8());
    System.out.println("response metadata: " + payload.getMetadataUtf8());

    assertThat(responseMessage).isEqualTo(payload.getDataUtf8());
  }

  @Test
  void testFragmentRequestMetaDataOnly() {
    System.out.println(
        "-------------------------------------------------testFragmentRequestMetaDataOnly-------------------------------------------------");
    handler =
        new AbstractRSocket() {
          @Override
          public Flux<Payload> requestStream(Payload payload) {
            String request = payload.getDataUtf8();
            String metaData = payload.getMetadataUtf8();
            System.out.println("request message:   " + request);
            System.out.println("request metadata:  " + metaData);

            return Flux.just(DefaultPayload.create(responseMessage));
          }
        };

    RSocket client = buildClient();

    System.out.println("original message:  " + message);
    System.out.println("original metadata: " + metaData);
    Payload payload = client.requestStream(DefaultPayload.create(message, metaData)).blockLast();
    System.out.println("response message:  " + payload.getDataUtf8());
    System.out.println("response metadata: " + payload.getMetadataUtf8());

    assertThat(responseMessage).isEqualTo(payload.getDataUtf8());
  }

  @Test
  void testFragmentBothMetaData() {
    Payload responsePayload = DefaultPayload.create(responseMessage);
    System.out.println(
        "-------------------------------------------------testFragmentBothMetaData-------------------------------------------------");
    handler =
        new AbstractRSocket() {
          @Override
          public Flux<Payload> requestStream(Payload payload) {
            String request = payload.getDataUtf8();
            String metaData = payload.getMetadataUtf8();
            System.out.println("request message:   " + request);
            System.out.println("request metadata:  " + metaData);

            return Flux.just(DefaultPayload.create(responseMessage, metaData));
          }

          @Override
          public Mono<Payload> requestResponse(Payload payload) {
            String request = payload.getDataUtf8();
            String metaData = payload.getMetadataUtf8();
            System.out.println("request message:   " + request);
            System.out.println("request metadata:  " + metaData);

            return Mono.just(DefaultPayload.create(responseMessage, metaData));
          }
        };

    RSocket client = buildClient();

    System.out.println("original message:  " + message);
    System.out.println("original metadata: " + metaData);
    Payload payload = client.requestStream(DefaultPayload.create(message, metaData)).blockLast();
    System.out.println("response message:  " + payload.getDataUtf8());
    System.out.println("response metadata: " + payload.getMetadataUtf8());

    assertThat(responseMessage).isEqualTo(payload.getDataUtf8());
  }
}
