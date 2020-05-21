package io.rsocket.addons;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class LoadBalancedRSocket implements RSocket {

  private final RSocketPool rSocketPool;

  public LoadBalancedRSocket(RSocketPool rSocketPool) {
    this.rSocketPool = rSocketPool;
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    return rSocketPool.select().fireAndForget(payload);
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    return rSocketPool.select().requestResponse(payload);
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    return rSocketPool.select().requestStream(payload);
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return rSocketPool.select().requestChannel(payloads);
  }

  @Override
  public Mono<Void> metadataPush(Payload payload) {
    return rSocketPool.select().metadataPush(payload);
  }

  @Override
  public void dispose() {
    rSocketPool.dispose();
  }

  @Override
  public Mono<Void> onClose() {
    return rSocketPool.onClose();
  }
}
