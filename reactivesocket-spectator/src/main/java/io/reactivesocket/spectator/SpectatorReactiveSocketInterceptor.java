package io.reactivesocket.spectator;

import com.netflix.spectator.api.Registry;
import io.reactivesocket.ClientReactiveSocket;
import io.reactivesocket.Plugins;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.ServerReactiveSocket;
import reactor.core.publisher.Mono;

/**
 * Interceptor that wraps a {@link ReactiveSocket} with a {@link SpectatorReactiveSocket}
 */
public class SpectatorReactiveSocketInterceptor implements Plugins.ReactiveSocketInterceptor {
    private static final String[] EMPTY = new String[0];
    private final Registry registry;
    private final String[] tags;

    public SpectatorReactiveSocketInterceptor(Registry registry, String... tags) {
        this.registry = registry;
        this.tags = tags;
    }

    public SpectatorReactiveSocketInterceptor(Registry registry) {
        this(registry, EMPTY);
    }

    @Override
    public Mono<ReactiveSocket> apply(ReactiveSocket reactiveSocket) {
        String[] t = tags;
        if (reactiveSocket instanceof ClientReactiveSocket) {
            t = SpectatorReactiveSocket.concatenate(t, "client");
        } else if (reactiveSocket instanceof ServerReactiveSocket) {
            t = SpectatorReactiveSocket.concatenate(t, "server");
        }

        return Mono.just(new SpectatorReactiveSocket(registry, reactiveSocket, t));


    }
}
