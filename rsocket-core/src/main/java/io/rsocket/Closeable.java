package io.rsocket;

import reactor.core.publisher.Mono;

/** */
public interface Closeable {
  /**
   * Close this {@code RSocket} upon subscribing to the returned {@code Publisher}
   *
   * <p><em>This method is idempotent and hence can be called as many times at any point with same
   * outcome.</em>
   *
   * @return A {@code Publisher} that completes when this {@code RSocket} close is complete.
   */
  Mono<Void> close();

  /**
   * Returns a {@code Publisher} that completes when this {@code RSocket} is closed. A {@code
   * RSocket} can be closed by explicitly calling {@link #close()} or when the underlying transport
   * connection is closed.
   *
   * @return A {@code Publisher} that completes when this {@code RSocket} close is complete.
   */
  Mono<Void> onClose();
}
