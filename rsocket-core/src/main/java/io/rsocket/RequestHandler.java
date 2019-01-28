package io.rsocket;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

/**
 * Extends the {@link RSocket} that allows an implementer to peek at the first request payload of a
 * channel.
 */
public interface RequestHandler extends RSocket {
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
