package io.rsocket;

import java.util.function.BiFunction;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

/**
 * Extends the {@link RSocket} that allows an implementer to peek at the first request payload of a
 * channel.
 *
 * @deprecated as of 1.0 RC7 in favor of using {@link RSocket#requestChannel(Publisher)} with {@link
 *     Flux#switchOnFirst(BiFunction)}
 */
@Deprecated
public interface ResponderRSocket extends RSocket {
  /**
   * Implement this method to peak at the first payload of the incoming request stream without
   * having to subscribe to Publish&lt;Payload&gt; payloads
   *
   * @param payload First payload in the stream - this is the same payload as the first payload in
   *     Publisher&lt;Payload&gt; payloads
   * @param payloads Stream of request payloads.
   * @return Stream of response payloads.
   */
  default Flux<Payload> requestChannel(Payload payload, Publisher<Payload> payloads) {
    return requestChannel(payloads);
  }
}
