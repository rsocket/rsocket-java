package io.rsocket.spectator;

import com.netflix.spectator.api.Registry;
import io.rsocket.RSocket;
import io.rsocket.plugins.RSocketInterceptor;

/** Interceptor that wraps a {@link RSocket} with a {@link SpectatorRSocket} */
public class SpectatorRSocketInterceptor implements RSocketInterceptor {
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
  public RSocket apply(RSocket reactiveSocket) {
    return new SpectatorRSocket(registry, reactiveSocket, tags);
  }
}
