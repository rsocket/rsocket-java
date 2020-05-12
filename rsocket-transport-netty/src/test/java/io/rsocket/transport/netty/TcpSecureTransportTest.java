package io.rsocket.transport.netty;

import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.rsocket.test.TransportTest;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import java.net.InetSocketAddress;
import java.security.cert.CertificateException;
import java.time.Duration;
import reactor.core.Exceptions;
import reactor.netty.tcp.TcpClient;
import reactor.netty.tcp.TcpServer;

public class TcpSecureTransportTest implements TransportTest {
  private final TransportPair transportPair =
      new TransportPair<>(
          () -> new InetSocketAddress("localhost", 0),
          (address, server) ->
              TcpClientTransport.create(
                  TcpClient.create()
                      .remoteAddress(server::address)
                      .secure(
                          ssl ->
                              ssl.sslContext(
                                  SslContextBuilder.forClient()
                                      .trustManager(InsecureTrustManagerFactory.INSTANCE)))),
          address -> {
            try {
              SelfSignedCertificate ssc = new SelfSignedCertificate();
              TcpServer server =
                  TcpServer.create()
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

  @Override
  public Duration getTimeout() {
    return Duration.ofMinutes(10);
  }

  @Override
  public TransportPair getTransportPair() {
    return transportPair;
  }
}
