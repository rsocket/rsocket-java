package io.rsocket.uri;

import io.rsocket.DuplexConnection;
import io.rsocket.transport.ClientTransport;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

public class UriClientTransport implements ClientTransport {
    private List<UriHandler> handlers;

    public UriClientTransport(ServiceLoader<UriHandler> services) {
        handlers = new ArrayList<>();
        handlers.forEach(handlers::add);
    }

    @Override
    public Mono<DuplexConnection> connect() {
        return Mono.error(new RuntimeException());
    }

    public static UriClientTransport fromServices() {
        ServiceLoader<UriHandler> services = ServiceLoader.load(UriHandler.class, UriHandler.class.getClassLoader());

        return new UriClientTransport(services);
    }
}
