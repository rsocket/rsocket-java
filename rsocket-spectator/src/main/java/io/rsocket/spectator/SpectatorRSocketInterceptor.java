package io.rsocket.spectator;

import com.netflix.spectator.api.Registry;
import io.rsocket.ClientRSocket;
import io.rsocket.Plugins;
import io.rsocket.RSocket;
import io.rsocket.ServerRSocket;
import reactor.core.publisher.Mono;

/**
 * Interceptor that wraps a {@link RSocket} with a {@link SpectatorRSocket}
 */
public class SpectatorRSocketInterceptor implements Plugins.RSocketInterceptor {
    private static final String[] EMPTY = new String[0];
    private final Registry registry;
    private final String[] tags;

    public SpectatorRSocketInterceptor(Registry registry, String... tags) {
        this.registry = registry;
        this.tags = tags;
    }

    public SpectatorRSocketInterceptor(Registry registry) {
        this(registry, EMPTY);
    }

    @Override
    public Mono<RSocket> apply(RSocket reactiveSocket) {
        String[] t = tags;
        if (reactiveSocket instanceof ClientRSocket) {
            t = SpectatorRSocket.concatenate(t, "client");
        } else if (reactiveSocket instanceof ServerRSocket) {
            t = SpectatorRSocket.concatenate(t, "server");
        }

        return Mono.just(new SpectatorRSocket(registry, reactiveSocket, t));


    }
}
