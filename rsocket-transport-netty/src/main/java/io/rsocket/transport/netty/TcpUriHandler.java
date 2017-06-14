package io.rsocket.transport.netty;

import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.uri.UriHandler;
import java.net.URI;
import java.util.Optional;
import reactor.ipc.netty.tcp.TcpServer;

public class TcpUriHandler implements UriHandler {
  @Override
  public Optional<ClientTransport> buildClient(URI uri) {
    if (uri.getScheme().equals("tcp")) {
      return Optional.of(TcpClientTransport.create(uri.getHost(), uri.getPort()));
    }

    return UriHandler.super.buildClient(uri);
  }

  @Override
  public Optional<ServerTransport> buildServer(URI uri) {
    if (uri.getScheme().equals("tcp")) {
      return Optional.of(TcpServerTransport.create(TcpServer.create(uri.getHost(), uri.getPort())));
    }

    return UriHandler.super.buildServer(uri);
  }
}
