/*
 * Copyright 2015-2021 the original author or authors.
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
import io.rsocket.core.RSocketClient;
import io.rsocket.core.RSocketConnector;
import io.rsocket.transport.ClientTransport;
import java.util.List;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

/**
 * An implementation of {@link RSocketClient backed by a pool of {@code RSocket} instances and using a {@link
 * LoadbalanceStrategy} to select the {@code RSocket} to use for a given request.
 *
 * @since 1.1
 */
public class LoadbalanceRSocketClient implements RSocketClient {

  private final RSocketPool rSocketPool;

  private LoadbalanceRSocketClient(RSocketPool rSocketPool) {
    this.rSocketPool = rSocketPool;
  }

  @Override
  public Mono<Void> onClose() {
    return rSocketPool.onClose();
  }

  /** Return {@code Mono} that selects an RSocket from the underlying pool. */
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

  /**
   * Shortcut to create an {@link LoadbalanceRSocketClient} with round-robin load balancing.
   * Effectively a shortcut for:
   *
   * <pre class="cdoe">
   * LoadbalanceRSocketClient.builder(targetPublisher)
   *    .connector(RSocketConnector.create())
   *    .build();
   * </pre>
   *
   * @param connector a "template" for connecting to load balance targets
   * @param targetPublisher refreshes the list of load balance targets periodically
   * @return the created client instance
   */
  public static LoadbalanceRSocketClient create(
      RSocketConnector connector, Publisher<List<LoadbalanceTarget>> targetPublisher) {
    return builder(targetPublisher).connector(connector).build();
  }

  /**
   * Return a builder for a {@link LoadbalanceRSocketClient}.
   *
   * @param targetPublisher refreshes the list of load balance targets periodically
   * @return the created builder
   */
  public static Builder builder(Publisher<List<LoadbalanceTarget>> targetPublisher) {
    return new Builder(targetPublisher);
  }

  /** Builder for creating an {@link LoadbalanceRSocketClient}. */
  public static class Builder {

    private final Publisher<List<LoadbalanceTarget>> targetPublisher;

    @Nullable private RSocketConnector connector;

    @Nullable LoadbalanceStrategy loadbalanceStrategy;

    Builder(Publisher<List<LoadbalanceTarget>> targetPublisher) {
      this.targetPublisher = targetPublisher;
    }

    /**
     * Configure the "template" connector to use for connecting to load balance targets. To
     * establish a connection, the {@link LoadbalanceTarget#getTransport() ClientTransport}
     * contained in each target is passed to the connector's {@link
     * RSocketConnector#connect(ClientTransport) connect} method and thus the same connector with
     * the same settings applies to all targets.
     *
     * <p>By default this is initialized with {@link RSocketConnector#create()}.
     *
     * @param connector the connector to use as a template
     */
    public Builder connector(RSocketConnector connector) {
      this.connector = connector;
      return this;
    }

    /**
     * Configure {@link RoundRobinLoadbalanceStrategy} as the strategy to use to select targets.
     *
     * <p>This is the strategy used by default.
     */
    public Builder roundRobinLoadbalanceStrategy() {
      this.loadbalanceStrategy = new RoundRobinLoadbalanceStrategy();
      return this;
    }

    /**
     * Configure {@link WeightedLoadbalanceStrategy} as the strategy to use to select targets.
     *
     * <p>By default, {@link RoundRobinLoadbalanceStrategy} is used.
     */
    public Builder weightedLoadbalanceStrategy() {
      this.loadbalanceStrategy = WeightedLoadbalanceStrategy.create();
      return this;
    }

    /**
     * Configure the {@link LoadbalanceStrategy} to use.
     *
     * <p>By default, {@link RoundRobinLoadbalanceStrategy} is used.
     */
    public Builder loadbalanceStrategy(LoadbalanceStrategy strategy) {
      this.loadbalanceStrategy = strategy;
      return this;
    }

    /** Build the {@link LoadbalanceRSocketClient} instance. */
    public LoadbalanceRSocketClient build() {
      final RSocketConnector connector =
          (this.connector != null ? this.connector : RSocketConnector.create());

      final LoadbalanceStrategy strategy =
          (this.loadbalanceStrategy != null
              ? this.loadbalanceStrategy
              : new RoundRobinLoadbalanceStrategy());

      if (strategy instanceof ClientLoadbalanceStrategy) {
        ((ClientLoadbalanceStrategy) strategy).initialize(connector);
      }

      return new LoadbalanceRSocketClient(
          new RSocketPool(connector, this.targetPublisher, strategy));
    }
  }
}
