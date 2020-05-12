/*
 * Copyright 2015-2018 the original author or authors.
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

package io.rsocket.client;

import io.rsocket.Availability;
import io.rsocket.Closeable;
import io.rsocket.RSocket;
import io.rsocket.client.filter.RSocketSupplier;
import io.rsocket.loadbalance.LoadBalancedRSocketClient;
import io.rsocket.loadbalance.LoadbalanceTarget;
import io.rsocket.loadbalance.WeightedLoadbalanceStrategy;
import java.util.Collection;
import java.util.stream.Collectors;
import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

/**
 * An implementation of {@link Mono} that load balances across a pool of RSockets and emits one when
 * it is subscribed to
 *
 * <p>It estimates the load of each RSocket based on statistics collected.
 */
@Deprecated
public abstract class LoadBalancedRSocketMono extends Mono<RSocket>
    implements Availability, Closeable {

  private final MonoProcessor<Void> onClose = MonoProcessor.create();
  private final LoadBalancedRSocketClient loadBalancedRSocketClient;

  protected final Mono<RSocket> rSocketMono;

  private LoadBalancedRSocketMono(LoadBalancedRSocketClient loadBalancedRSocketClient) {
    this.rSocketMono = loadBalancedRSocketClient.source();
    this.loadBalancedRSocketClient = loadBalancedRSocketClient;
  }

  @Override
  public void dispose() {
    this.loadBalancedRSocketClient.dispose();
  }

  @Override
  public Mono<Void> onClose() {
    return onClose;
  }

  @Override
  public double availability() {
    return 1.0d;
  }

  @Deprecated
  public static LoadBalancedRSocketMono create(
      Publisher<? extends Collection<RSocketSupplier>> factories) {

    return fromClient(
        Flux.from(factories)
            .map(
                rsl ->
                    rsl.stream()
                        .map(rs -> new LoadbalanceTarget(rs.toString(), rs.get()))
                        .collect(Collectors.toList()))
            .as(f -> LoadBalancedRSocketClient.create(new WeightedLoadbalanceStrategy(), f)));
  }

  public static LoadBalancedRSocketMono fromClient(LoadBalancedRSocketClient rSocketClient) {
    return new LoadBalancedRSocketMono(rSocketClient) {
      @Override
      public void subscribe(CoreSubscriber<? super RSocket> s) {
        rSocketClient.source().subscribe(s);
      }
    };
  }
}
