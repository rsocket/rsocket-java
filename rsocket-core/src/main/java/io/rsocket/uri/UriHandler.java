package io.rsocket.uri;

import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;

import java.net.URI;
import java.util.Optional;

public interface UriHandler {
    // TODO should I use Mono here instead of Optional?
    default Optional<ClientTransport> buildClient(URI uri) {
        return Optional.empty();
    }

    default Optional<ServerTransport> buildServer(URI uri) {
        return Optional.empty();
    }
}
