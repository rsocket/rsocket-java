package io.rsocket.core;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketClient;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class LoadBalancedRSocketClient implements RSocketClient {

  private final RSocketPool rSocketPool;

  LoadBalancedRSocketClient(RSocketPool rSocketPool) {
    this.rSocketPool = rSocketPool;
  }

  @Override
  public Mono<RSocket> source() {
    return Mono.just(rSocketPool.select());
  }

  @Override
  public Mono<Void> fireAndForget(Mono<Payload> payloadMono) {
    return payloadMono.flatMap(p -> rSocketPool.select().fireAndForget(p));
  }

  @Override
  public Mono<Payload> requestResponse(Mono<Payload> payloadMono) {
    return payloadMono.flatMap(p -> rSocketPool.select().requestResponse(p));
  }

  @Override
  public Flux<Payload> requestStream(Mono<Payload> payloadMono) {
    return payloadMono.flatMapMany(p -> rSocketPool.select().requestStream(p));
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    return rSocketPool.select().requestChannel(payloads);
  }

  @Override
  public Mono<Void> metadataPush(Mono<Payload> payloadMono) {
    return payloadMono.flatMap(p -> rSocketPool.select().metadataPush(p));
  }

  @Override
  public void dispose() {
    rSocketPool.dispose();
  }
}
