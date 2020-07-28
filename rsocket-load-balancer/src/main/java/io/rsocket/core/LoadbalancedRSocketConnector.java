package io.rsocket.core;

import io.rsocket.RSocketClient;
import io.rsocket.stat.Quantile;
import java.util.List;
import java.util.function.Supplier;
import org.reactivestreams.Publisher;

public class LoadbalancedRSocketConnector {
  Supplier<Stats> statsSupplier = () -> Stats.NoOpsStats.INSTANCE;
  LoadbalanceStrategy loadbalanceStrategy = new RoundRobinLoadbalanceStrategy();

  public static LoadbalancedRSocketConnector builder() {
    return new LoadbalancedRSocketConnector();
  }

  public LoadbalancedRSocketConnector withoutStatsTracking() {
    this.statsSupplier = () -> Stats.NoOpsStats.INSTANCE;

    return this;
  }

  public LoadbalancedRSocketConnector withStatsTracking() {
    this.statsSupplier = Stats::new;

    return this;
  }

  public LoadbalancedRSocketConnector withStatsTracking(
      Quantile lowerQuantile, Quantile higherQuantile, long inactivityFactor) {
    this.statsSupplier = () -> new Stats(lowerQuantile, higherQuantile, inactivityFactor);

    return this;
  }

  public LoadbalancedRSocketConnector withLoadbalanceStrategy(
      LoadbalanceStrategy loadbalanceStrategy) {
    this.loadbalanceStrategy = loadbalanceStrategy;

    return this;
  }

  public RSocketClient createClient(Publisher<List<RSocketSupplier>> rSocketSuppliersPublisher) {
    return new LoadBalancedRSocketClient(
        new RSocketPool(rSocketSuppliersPublisher, loadbalanceStrategy, statsSupplier));
  }
}
