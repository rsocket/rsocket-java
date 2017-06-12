package io.rsocket.transport.netty;

import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.uri.UriHandler;

import java.net.URI;
import java.util.Optional;

public class TcpUriHandler implements UriHandler {
    @Override
    public Optional<ClientTransport> buildClient(URI uri) {
        if (uri.getScheme().equals("tcp")) {
            return Optional.of(TcpClientTransport.create(uri.getHost(), uri.getPort()));
        }

        return UriHandler.super.buildClient(uri);
    }
}
