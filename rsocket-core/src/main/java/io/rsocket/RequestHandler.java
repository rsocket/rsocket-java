package io.rsocket;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

/**
 * Extends the {@link RSocket} that allows an implementor to peek at the first request payload of a channel.
 */
public interface RequestHandler extends RSocket {
  /**
   * Implement this method to peak at the first payload of the incoming request stream
   * @param payload First payload in the stream
   * @param payloads Stream of request payloads.
   * @return Stream of response payloads.
   */
  default Flux<Payload> requestChannel(Payload payload, Publisher<Payload> payloads) {
    return requestChannel(payloads);
  }
}
