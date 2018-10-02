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

package io.rsocket.transport.netty;

import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.rsocket.test.TransportTest;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import io.rsocket.transport.netty.server.WebsocketServerTransport;
import java.net.InetSocketAddress;
import java.security.cert.CertificateException;
import java.time.Duration;

import reactor.core.Exceptions;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.server.HttpServer;
import reactor.netty.tcp.TcpServer;

final class WebsocketSecureTransportTest implements TransportTest {

  private final TransportPair transportPair =
      new TransportPair<>(
          () -> new InetSocketAddress("localhost", 0),
          (address, server) ->
              WebsocketClientTransport.create(
                  HttpClient.create()
                    .addressSupplier(server::address)
                    .secure(ssl -> ssl.sslContext(
                        SslContextBuilder.forClient()
                            .trustManager(InsecureTrustManagerFactory.INSTANCE))),
                  String.format(
                      "https://%s:%d/",
                      server.address().getHostName(), server.address().getPort())),
          address -> {
            try {
              SelfSignedCertificate ssc = new SelfSignedCertificate();
              HttpServer server = HttpServer.from(
                  TcpServer.create()
                      .addressSupplier(() -> address)
                      .secure(ssl -> ssl.sslContext(
                          SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()))));
              return WebsocketServerTransport.create(server);
            } catch (CertificateException e) {
              throw Exceptions.propagate(e);
            }
          });

  @Override
  public Duration getTimeout() {
    return Duration.ofMinutes(3);
  }

  @Override
  public TransportPair getTransportPair() {
    return transportPair;
  }
}
