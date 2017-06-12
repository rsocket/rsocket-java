package io.rsocket.uri;

import io.rsocket.transport.ClientTransport;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;

public class UriTransportRegistry {
    private static final ClientTransport FAILED = () -> Mono.error(new UnsupportedOperationException());

    private List<UriHandler> handlers;

    public UriTransportRegistry(ServiceLoader<UriHandler> services) {
        handlers = new ArrayList<>();
        services.forEach(handlers::add);
    }

    public static UriTransportRegistry fromServices() {
        ServiceLoader<UriHandler> services = ServiceLoader.load(UriHandler.class);

        return new UriTransportRegistry(services);
    }

    public static ClientTransport forUri(String uri) {
        return UriTransportRegistry.fromServices().find(uri);
    }

    private ClientTransport find(String uriString) {
        URI uri = URI.create(uriString);

        for (UriHandler h: handlers) {
            Optional<ClientTransport> r = h.buildClient(uri);
            if (r.isPresent()) {
                return r.get();
            }
        }

        return FAILED;
    }
}
