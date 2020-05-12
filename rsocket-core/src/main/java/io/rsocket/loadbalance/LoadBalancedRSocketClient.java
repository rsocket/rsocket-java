/*
 * Copyright 2015-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.rsocket.loadbalance;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketClient;
import java.util.List;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class LoadBalancedRSocketClient implements RSocketClient {

  private final RSocketPool rSocketPool;

  LoadBalancedRSocketClient(RSocketPool rSocketPool) {
    this.rSocketPool = rSocketPool;
  }

  @Override
  public Mono<RSocket> source() {
    return Mono.fromSupplier(rSocketPool::select);
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

  public static LoadBalancedRSocketClient create(
      LoadbalanceStrategy loadbalanceStrategy,
      Publisher<List<LoadbalanceTarget>> rSocketSuppliersPublisher) {
    return new LoadBalancedRSocketClient(
        new RSocketPool(rSocketSuppliersPublisher, loadbalanceStrategy));
  }

  public static RSocketClient create(Publisher<List<LoadbalanceTarget>> rSocketSuppliersPublisher) {
    return create(new RoundRobinLoadbalanceStrategy(), rSocketSuppliersPublisher);
  }
}
