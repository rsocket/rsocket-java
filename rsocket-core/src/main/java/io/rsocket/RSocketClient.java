package io.rsocket;

import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * A client-side interface to simplify interactions with the {@link
 * io.rsocket.core.RSocketConnector}. This interface represents logical communication over {@link
 * RSocket}, hiding the complexity of {@code Mono<RSocket>} resolution. Also, in opposite to {@link
 * RSocket} , {@link RSocketClient} supports multi-subscription on the same {@link Publisher} from
 * the interaction in the way of accepting input as {@link Publisher} like {@link Mono} or {@link
 * Flux} Despite, {@link RSocket} interface, {@link RSocketClient} does not coupled with a single
 * connection, hence disposal of the {@link #source()} {@link RSocket} will affect the {@link
 * RSocketClient} it selves. In such a case, a new request will cause automatic reconnection if
 * necessary.
 *
 * @since 1.0.1
 */
public interface RSocketClient extends Disposable {

  /**
   * Provides access to the source {@link RSocket} used by this {@link RSocketClient}
   *
   * @return returns a {@link Mono} which returns the source {@link RSocket}
   */
  Mono<RSocket> source();

  /**
   * Fire and Forget interaction model of {@link RSocketClient}.
   *
   * @param payloadMono Request payload as {@link Mono}.
   * @return {@code Publisher} that completes when the passed {@code payload} is successfully
   *     handled, otherwise errors.
   */
  Mono<Void> fireAndForget(Mono<Payload> payloadMono);

  /**
   * Request-Response interaction model of {@link RSocketClient}.
   *
   * @param payloadMono Request payload as {@link Mono}.
   * @return {@code Publisher} containing at most a single {@code Payload} representing the
   *     response.
   */
  Mono<Payload> requestResponse(Mono<Payload> payloadMono);

  /**
   * Request-Stream interaction model of {@link RSocketClient}.
   *
   * @param payloadMono Request payload as {@link Mono}.
   * @return {@code Publisher} containing the stream of {@code Payload}s representing the response.
   */
  Flux<Payload> requestStream(Mono<Payload> payloadMono);

  /**
   * Request-Channel interaction model of {@link RSocketClient}.
   *
   * @param payloads Stream of request payloads.
   * @return Stream of response payloads.
   */
  Flux<Payload> requestChannel(Publisher<Payload> payloads);

  /**
   * Metadata-Push interaction model of {@link RSocketClient}.
   *
   * @param payloadMono Request payload as {@link Mono}.
   * @return {@code Publisher} that completes when the passed {@code payload} is successfully
   *     handled, otherwise errors.
   */
  Mono<Void> metadataPush(Mono<Payload> payloadMono);
}
