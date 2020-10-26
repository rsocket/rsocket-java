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
import io.rsocket.core.RSocketClient;
import io.rsocket.core.RSocketConnector;
import java.util.List;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

/**
 * {@link RSocketClient} implementation that uses a {@link LoadbalanceStrategy} to select the {@code
 * RSocket} to use for a given request from a pool of possible targets.
 *
 * @since 1.1
 */
public class LoadbalanceRSocketClient implements RSocketClient {

  private final RSocketPool rSocketPool;

  private LoadbalanceRSocketClient(RSocketPool rSocketPool) {
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

  /**
   * Shortcut to create an {@link LoadbalanceRSocketClient} with round robin loadalancing.
   * Effectively a shortcut for:
   *
   * <pre class="cdoe">
   * LoadbalanceRSocketClient.builder(targetPublisher)
   *    .connector(RSocketConnector.create())
   *    .build();
   * </pre>
   *
   * @param connector the {@link Builder#connector(RSocketConnector) to use
   * @param targetPublisher publisher that periodically refreshes the list of targets to loadbalance across.
   * @return the created client instance
   */
  public static LoadbalanceRSocketClient create(
      RSocketConnector connector, Publisher<List<LoadbalanceTarget>> targetPublisher) {
    return builder(targetPublisher).connector(connector).build();
  }

  /**
   * Return a builder to create an {@link LoadbalanceRSocketClient} with.
   *
   * @param targetPublisher publisher that periodically refreshes the list of targets to loadbalance
   *     across.
   * @return the builder instance
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
     * The given {@link RSocketConnector} is used as a template to produce the {@code Mono<RSocket>}
     * source for each {@link LoadbalanceTarget}. This is done by passing the {@code
     * ClientTransport} contained in every target to the {@code connect} method of the given
     * connector instance.
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
     * Switch to using a round-robin strategy for selecting a target.
     *
     * <p>This is the strategy used by default.
     */
    public Builder roundRobinLoadbalanceStrategy() {
      this.loadbalanceStrategy = new RoundRobinLoadbalanceStrategy();
      return this;
    }

    /**
     * Switch to using a strategy that assigns a weight to each pooled {@code RSocket} based on
     * actual usage stats, and uses that to make a choice.
     *
     * <p>By default, {@link RoundRobinLoadbalanceStrategy} is used.
     */
    public Builder weightedLoadbalanceStrategy() {
      this.loadbalanceStrategy = WeightedLoadbalanceStrategy.create();
      return this;
    }

    /**
     * Provide the {@link LoadbalanceStrategy} to use.
     *
     * <p>By default, {@link RoundRobinLoadbalanceStrategy} is used.
     */
    public Builder loadbalanceStrategy(LoadbalanceStrategy strategy) {
      this.loadbalanceStrategy = strategy;
      return this;
    }

    /** Build the {@link LoadbalanceRSocketClient} instance. */
    public LoadbalanceRSocketClient build() {
      final RSocketConnector connector = initConnector();
      final LoadbalanceStrategy strategy = initLoadbalanceStrategy();

      if (strategy instanceof ClientLoadbalanceStrategy) {
        ((ClientLoadbalanceStrategy) strategy).initialize(connector);
      }

      return new LoadbalanceRSocketClient(
          new RSocketPool(connector, this.targetPublisher, strategy));
    }

    private RSocketConnector initConnector() {
      return (this.connector != null ? this.connector : RSocketConnector.create());
    }

    private LoadbalanceStrategy initLoadbalanceStrategy() {
      return (this.loadbalanceStrategy != null
          ? this.loadbalanceStrategy
          : new RoundRobinLoadbalanceStrategy());
    }
  }
}
