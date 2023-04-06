/*
 * Copyright 2015-2023 the original author or authors.
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

import io.netty.channel.ChannelOption;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.rsocket.test.TransportTest;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import java.net.InetSocketAddress;
import java.security.cert.CertificateException;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import reactor.core.Exceptions;
import reactor.netty.tcp.TcpClient;
import reactor.netty.tcp.TcpServer;

public class TcpSecureTransportTest implements TransportTest {
  private TransportPair transportPair;

  @BeforeEach
  void createTestPair() {
    transportPair =
        new TransportPair<>(
            () -> new InetSocketAddress("localhost", 0),
            (address, server, allocator) ->
                TcpClientTransport.create(
                    TcpClient.create()
                        .option(ChannelOption.ALLOCATOR, allocator)
                        .remoteAddress(server::address)
                        .secure(
                            ssl ->
                                ssl.sslContext(
                                    SslContextBuilder.forClient()
                                        .trustManager(InsecureTrustManagerFactory.INSTANCE)))),
            (address, allocator) -> {
              try {
                SelfSignedCertificate ssc = new SelfSignedCertificate();
                TcpServer server =
                    TcpServer.create()
                        .option(ChannelOption.ALLOCATOR, allocator)
                        .bindAddress(() -> address)
                        .secure(
                            ssl ->
                                ssl.sslContext(
                                    SslContextBuilder.forServer(
                                        ssc.certificate(), ssc.privateKey())));
                return TcpServerTransport.create(server);
              } catch (CertificateException e) {
                throw Exceptions.propagate(e);
              }
            });
  }

  @Override
  public Duration getTimeout() {
    return Duration.ofMinutes(10);
  }

  @Override
  public TransportPair getTransportPair() {
    return transportPair;
  }
}
