package io.rsocket.transport.netty;

import io.netty.channel.ChannelOption;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.rsocket.test.TransportTest;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import java.net.InetSocketAddress;
import java.security.cert.CertificateException;
import java.time.Duration;
import reactor.core.Exceptions;
import reactor.netty.tcp.TcpClient;
import reactor.netty.tcp.TcpServer;
import reactor.netty.tcp.TcpSslContextSpec;

public class TcpSecureTransportTest extends TransportTest<InetSocketAddress, CloseableChannel> {

  @Override
  protected TransportPair<InetSocketAddress, CloseableChannel> createTransportPair() {
    return new TransportPair<>(
        () -> new InetSocketAddress("localhost", 0),
        (address, server, allocator) ->
            TcpClientTransport.create(
                TcpClient.create()
                    .option(ChannelOption.ALLOCATOR, allocator)
                    .remoteAddress(server::address)
                    .secure(
                        ssl ->
                            ssl.sslContext(
                                TcpSslContextSpec.forClient()
                                    .configure(
                                        scb ->
                                            scb.trustManager(
                                                InsecureTrustManagerFactory.INSTANCE))))),
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
                                TcpSslContextSpec.forServer(ssc.certificate(), ssc.privateKey())));
            return TcpServerTransport.create(server);
          } catch (CertificateException e) {
            throw Exceptions.propagate(e);
          }
        },
        Duration.ofMinutes(2));
  }
}
