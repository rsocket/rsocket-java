package io.rsocket.transport.local;

import io.rsocket.DuplexConnection;
import io.rsocket.Frame;
import io.rsocket.transport.TransportClient;
import io.rsocket.transport.local.LocalServer.StartServerImpl;
import reactor.core.publisher.UnicastProcessor;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

public class LocalClient implements TransportClient {
    private final String name;

    private LocalClient(String name) {
        this.name = name;
    }

    public static LocalClient create(String name) {
        return new LocalClient(name);
    }

    @Override
    public Mono<DuplexConnection> connect() {
        return Mono.defer(() -> {
            StartServerImpl server = LocalServer.findServer(name);
            if (server != null) {
                final UnicastProcessor<Frame> in = UnicastProcessor.create();
                final UnicastProcessor<Frame> out = UnicastProcessor.create();
                final MonoProcessor<Void> closeNotifier = MonoProcessor.create();
                server.accept(new LocalDuplexConnection(out, in, closeNotifier));
                DuplexConnection client = new LocalDuplexConnection(in, out, closeNotifier);
                return Mono.just(client);
            }
            return Mono.error(new IllegalArgumentException("Could not find server: " + name));
        });
    }
}
