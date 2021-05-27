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

import io.rsocket.*;
import io.rsocket.client.filter.RSocketSupplier;
import io.rsocket.stat.Ewma;
import io.rsocket.stat.FrugalQuantile;
import io.rsocket.stat.Median;
import io.rsocket.stat.Quantile;
import io.rsocket.util.Clock;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.Operators;
import reactor.util.context.Context;
import reactor.util.retry.Retry;

/**
 * An implementation of {@link Mono} that load balances across a pool of RSockets and emits one when
 * it is subscribed to
 *
 * <p>It estimates the load of each RSocket based on statistics collected.
 *
 * @deprecated as of 1.1. in favor of {@link io.rsocket.loadbalance.LoadbalanceRSocketClient}.
 */
@Deprecated
public abstract class LoadBalancedRSocketMono extends Mono<RSocket>
    implements Availability, Closeable {

  public static final double DEFAULT_EXP_FACTOR = 4.0;
  public static final double DEFAULT_LOWER_QUANTILE = 0.2;
  public static final double DEFAULT_HIGHER_QUANTILE = 0.8;
  public static final double DEFAULT_MIN_PENDING = 1.0;
  public static final double DEFAULT_MAX_PENDING = 2.0;
  public static final int DEFAULT_MIN_APERTURE = 3;
  public static final int DEFAULT_MAX_APERTURE = 100;
  public static final long DEFAULT_MAX_REFRESH_PERIOD_MS =
      TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);
  private static final Logger logger = LoggerFactory.getLogger(LoadBalancedRSocketMono.class);
  private static final long APERTURE_REFRESH_PERIOD = Clock.unit().convert(15, TimeUnit.SECONDS);
  private static final int EFFORT = 5;
  private static final long DEFAULT_INITIAL_INTER_ARRIVAL_TIME =
      Clock.unit().convert(1L, TimeUnit.SECONDS);
  private static final int DEFAULT_INTER_ARRIVAL_FACTOR = 500;

  private static final FailingRSocket FAILING_REACTIVE_SOCKET = new FailingRSocket();
  protected final Mono<RSocket> rSocketMono;
  private final double minPendings;
  private final double maxPendings;
  private final int minAperture;
  private final int maxAperture;
  private final long maxRefreshPeriod;
  private final double expFactor;
  private final Quantile lowerQuantile;
  private final Quantile higherQuantile;
  private final ArrayList<WeightedSocket> activeSockets;
  private final Ewma pendings;
  private final MonoProcessor<Void> onClose = MonoProcessor.create();
  private final RSocketSupplierPool pool;
  private final long weightedSocketRetries;
  private final Duration weightedSocketBackOff;
  private final Duration weightedSocketMaxBackOff;
  private volatile int targetAperture;
  private long lastApertureRefresh;
  private long refreshPeriod;
  private int pendingSockets;
  private volatile long lastRefresh;

  /**
   * @param factories the source (factories) of RSocket
   * @param expFactor how aggressive is the algorithm toward outliers. A higher number means we send
   *     aggressively less traffic to a server slightly slower.
   * @param lowQuantile the lower bound of the latency band of acceptable values. Any server below
   *     that value will be aggressively favored.
   * @param highQuantile the higher bound of the latency band of acceptable values. Any server above
   *     that value will be aggressively penalized.
   * @param minPendings The lower band of the average outstanding messages per server.
   * @param maxPendings The higher band of the average outstanding messages per server.
   * @param minAperture the minimum number of connections we want to maintain, independently of the
   *     load.
   * @param maxAperture the maximum number of connections we want to maintain, independently of the
   *     load.
   * @param maxRefreshPeriodMs the maximum time between two "refreshes" of the list of active
   *     RSocket. This is at that time that the slowest RSocket is closed. (unit is millisecond)
   * @param weightedSocketRetries the number of times a weighted socket will attempt to retry when
   *     it receives an error before reconnecting. The default is 5 times.
   * @param weightedSocketBackOff the duration a a weighted socket will add to each retry attempt.
   * @param weightedSocketMaxBackOff the max duration a weighted socket will delay before retrying
   *     to connect. The default is 5 seconds.
   */
  private LoadBalancedRSocketMono(
      Publisher<? extends Collection<RSocketSupplier>> factories,
      double expFactor,
      double lowQuantile,
      double highQuantile,
      double minPendings,
      double maxPendings,
      int minAperture,
      int maxAperture,
      long maxRefreshPeriodMs,
      long weightedSocketRetries,
      Duration weightedSocketBackOff,
      Duration weightedSocketMaxBackOff) {
    this.weightedSocketRetries = weightedSocketRetries;
    this.weightedSocketBackOff = weightedSocketBackOff;
    this.weightedSocketMaxBackOff = weightedSocketMaxBackOff;
    this.expFactor = expFactor;
    this.lowerQuantile = new FrugalQuantile(lowQuantile);
    this.higherQuantile = new FrugalQuantile(highQuantile);

    this.activeSockets = new ArrayList<>();
    this.pendingSockets = 0;

    this.minPendings = minPendings;
    this.maxPendings = maxPendings;
    this.pendings = new Ewma(15, TimeUnit.SECONDS, (minPendings + maxPendings) / 2.0);

    this.minAperture = minAperture;
    this.maxAperture = maxAperture;
    this.targetAperture = minAperture;

    this.maxRefreshPeriod = Clock.unit().convert(maxRefreshPeriodMs, TimeUnit.MILLISECONDS);
    this.lastApertureRefresh = Clock.now();
    this.refreshPeriod = Clock.unit().convert(15L, TimeUnit.SECONDS);
    this.lastRefresh = Clock.now();
    this.pool = new RSocketSupplierPool(factories);
    refreshSockets();

    rSocketMono = Mono.fromSupplier(this::select);

    onClose.doFinally(signalType -> pool.dispose()).subscribe();
  }

  public static LoadBalancedRSocketMono create(
      Publisher<? extends Collection<RSocketSupplier>> factories) {
    return create(
        factories,
        DEFAULT_EXP_FACTOR,
        DEFAULT_LOWER_QUANTILE,
        DEFAULT_HIGHER_QUANTILE,
        DEFAULT_MIN_PENDING,
        DEFAULT_MAX_PENDING,
        DEFAULT_MIN_APERTURE,
        DEFAULT_MAX_APERTURE,
        DEFAULT_MAX_REFRESH_PERIOD_MS);
  }

  public static LoadBalancedRSocketMono create(
      Publisher<? extends Collection<RSocketSupplier>> factories,
      double expFactor,
      double lowQuantile,
      double highQuantile,
      double minPendings,
      double maxPendings,
      int minAperture,
      int maxAperture,
      long maxRefreshPeriodMs,
      long weightedSocketRetries,
      Duration weightedSocketBackOff,
      Duration weightedSocketMaxBackOff) {
    return new LoadBalancedRSocketMono(
        factories,
        expFactor,
        lowQuantile,
        highQuantile,
        minPendings,
        maxPendings,
        minAperture,
        maxAperture,
        maxRefreshPeriodMs,
        weightedSocketRetries,
        weightedSocketBackOff,
        weightedSocketMaxBackOff) {
      @Override
      public void subscribe(CoreSubscriber<? super RSocket> s) {
        rSocketMono.subscribe(s);
      }
    };
  }

  public static LoadBalancedRSocketMono create(
      Publisher<? extends Collection<RSocketSupplier>> factories,
      double expFactor,
      double lowQuantile,
      double highQuantile,
      double minPendings,
      double maxPendings,
      int minAperture,
      int maxAperture,
      long maxRefreshPeriodMs) {
    return new LoadBalancedRSocketMono(
        factories,
        expFactor,
        lowQuantile,
        highQuantile,
        minPendings,
        maxPendings,
        minAperture,
        maxAperture,
        maxRefreshPeriodMs,
        5,
        Duration.ofMillis(500),
        Duration.ofSeconds(5)) {
      @Override
      public void subscribe(CoreSubscriber<? super RSocket> s) {
        rSocketMono.subscribe(s);
      }
    };
  }

  /**
   * Responsible for: - refreshing the aperture - asynchronously adding/removing reactive sockets to
   * match targetAperture - periodically append a new connection
   */
  private synchronized void refreshSockets() {
    refreshAperture();
    int n = activeSockets.size();
    if (n < targetAperture && !pool.isPoolEmpty()) {
      logger.debug(
          "aperture {} is below target {}, adding {} sockets",
          n,
          targetAperture,
          targetAperture - n);
      addSockets(targetAperture - n);
    } else if (targetAperture < activeSockets.size()) {
      logger.debug("aperture {} is above target {}, quicking 1 socket", n, targetAperture);
      quickSlowestRS();
    }

    long now = Clock.now();
    if (now - lastRefresh >= refreshPeriod) {
      long prev = refreshPeriod;
      refreshPeriod = (long) Math.min(refreshPeriod * 1.5, maxRefreshPeriod);
      logger.debug("Bumping refresh period, {}->{}", prev / 1000, refreshPeriod / 1000);
      lastRefresh = now;
      addSockets(1);
    }
  }

  private synchronized void addSockets(int numberOfNewSocket) {
    int n = numberOfNewSocket;
    int poolSize = pool.poolSize();
    if (n > poolSize) {
      n = poolSize;
      logger.debug(
          "addSockets({}) restricted by the number of factories, i.e. addSockets({})",
          numberOfNewSocket,
          n);
    }

    for (int i = 0; i < n; i++) {
      Optional<RSocketSupplier> optional = pool.get();

      if (optional.isPresent()) {
        RSocketSupplier supplier = optional.get();
        WeightedSocket socket = new WeightedSocket(supplier, lowerQuantile, higherQuantile);
      } else {
        break;
      }
    }
  }

  private synchronized void refreshAperture() {
    int n = activeSockets.size();
    if (n == 0) {
      return;
    }

    double p = 0.0;
    for (WeightedSocket wrs : activeSockets) {
      p += wrs.getPending();
    }
    p /= n + pendingSockets;
    pendings.insert(p);
    double avgPending = pendings.value();

    long now = Clock.now();
    boolean underRateLimit = now - lastApertureRefresh > APERTURE_REFRESH_PERIOD;
    if (avgPending < 1.0 && underRateLimit) {
      updateAperture(targetAperture - 1, now);
    } else if (2.0 < avgPending && underRateLimit) {
      updateAperture(targetAperture + 1, now);
    }
  }

  /**
   * Update the aperture value and ensure its value stays in the right range.
   *
   * @param newValue new aperture value
   * @param now time of the change (for rate limiting purposes)
   */
  private void updateAperture(int newValue, long now) {
    int previous = targetAperture;
    targetAperture = newValue;
    targetAperture = Math.max(minAperture, targetAperture);
    int maxAperture = Math.min(this.maxAperture, activeSockets.size() + pool.poolSize());
    targetAperture = Math.min(maxAperture, targetAperture);
    lastApertureRefresh = now;
    pendings.reset((minPendings + maxPendings) / 2);

    if (targetAperture != previous) {
      logger.debug(
          "Current pending={}, new target={}, previous target={}",
          pendings.value(),
          targetAperture,
          previous);
    }
  }

  private synchronized void quickSlowestRS() {
    if (activeSockets.size() <= 1) {
      return;
    }

    WeightedSocket slowest = null;
    double lowestAvailability = Double.MAX_VALUE;
    for (WeightedSocket socket : activeSockets) {
      double load = socket.availability();
      if (load == 0.0) {
        slowest = socket;
        break;
      }
      if (socket.getPredictedLatency() != 0) {
        load *= 1.0 / socket.getPredictedLatency();
      }
      if (load < lowestAvailability) {
        lowestAvailability = load;
        slowest = socket;
      }
    }

    if (slowest != null) {
      logger.debug("Disposing slowest WeightedSocket {}", slowest);
      slowest.dispose();
    }
  }

  @Override
  public synchronized double availability() {
    double currentAvailability = 0.0;
    if (!activeSockets.isEmpty()) {
      for (WeightedSocket rs : activeSockets) {
        currentAvailability += rs.availability();
      }
      currentAvailability /= activeSockets.size();
    }

    return currentAvailability;
  }

  private synchronized RSocket select() {
    refreshSockets();

    if (activeSockets.isEmpty()) {
      return FAILING_REACTIVE_SOCKET;
    }

    int size = activeSockets.size();
    if (size == 1) {
      return activeSockets.get(0);
    }

    WeightedSocket rsc1 = null;
    WeightedSocket rsc2 = null;

    Random rng = ThreadLocalRandom.current();
    for (int i = 0; i < EFFORT; i++) {
      int i1 = rng.nextInt(size);
      int i2 = rng.nextInt(size - 1);
      if (i2 >= i1) {
        i2++;
      }
      rsc1 = activeSockets.get(i1);
      rsc2 = activeSockets.get(i2);
      if (rsc1.availability() > 0.0 && rsc2.availability() > 0.0) {
        break;
      }
      if (i + 1 == EFFORT && !pool.isPoolEmpty()) {
        addSockets(1);
      }
    }

    double w1 = algorithmicWeight(rsc1);
    double w2 = algorithmicWeight(rsc2);
    if (w1 < w2) {
      return rsc2;
    } else {
      return rsc1;
    }
  }

  private double algorithmicWeight(WeightedSocket socket) {
    if (socket == null || socket.availability() == 0.0) {
      return 0.0;
    }

    int pendings = socket.getPending();
    double latency = socket.getPredictedLatency();

    double low = lowerQuantile.estimation();
    double high =
        Math.max(
            higherQuantile.estimation(),
            low * 1.001); // ensure higherQuantile > lowerQuantile + .1%
    double bandWidth = Math.max(high - low, 1);

    if (latency < low) {
      double alpha = (low - latency) / bandWidth;
      double bonusFactor = Math.pow(1 + alpha, expFactor);
      latency /= bonusFactor;
    } else if (latency > high) {
      double alpha = (latency - high) / bandWidth;
      double penaltyFactor = Math.pow(1 + alpha, expFactor);
      latency *= penaltyFactor;
    }

    return socket.availability() * 1.0 / (1.0 + latency * (pendings + 1));
  }

  @Override
  public synchronized String toString() {
    return "LoadBalancer(a:"
        + activeSockets.size()
        + ", f: "
        + pool.poolSize()
        + ", avgPendings="
        + pendings.value()
        + ", targetAperture="
        + targetAperture
        + ", band=["
        + lowerQuantile.estimation()
        + ", "
        + higherQuantile.estimation()
        + "])";
  }

  @Override
  public void dispose() {
    synchronized (this) {
      activeSockets.forEach(WeightedSocket::dispose);
      activeSockets.clear();
      onClose.onComplete();
    }
  }

  @Override
  public boolean isDisposed() {
    return onClose.isDisposed();
  }

  @Override
  public Mono<Void> onClose() {
    return onClose;
  }

  /**
   * (Null Object Pattern) This failing RSocket never succeed, it is useful for simplifying the code
   * when dealing with edge cases.
   */
  private static class FailingRSocket implements RSocket {

    private static final Mono<Void> errorVoid = Mono.error(NoAvailableRSocketException.INSTANCE);
    private static final Mono<Payload> errorPayload =
        Mono.error(NoAvailableRSocketException.INSTANCE);

    @Override
    public Mono<Void> fireAndForget(Payload payload) {
      return errorVoid;
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
      return errorPayload;
    }

    @Override
    public Flux<Payload> requestStream(Payload payload) {
      return errorPayload.flux();
    }

    @Override
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
      return errorPayload.flux();
    }

    @Override
    public Mono<Void> metadataPush(Payload payload) {
      return errorVoid;
    }

    @Override
    public SocketAddress localAddress() {
      throw new RuntimeException(NoAvailableRSocketException.INSTANCE);
    }

    @Override
    public SocketAddress remoteAddress() {
      throw new RuntimeException(NoAvailableRSocketException.INSTANCE);
    }

    @Override
    public double availability() {
      return 0;
    }

    @Override
    public void dispose() {}

    @Override
    public boolean isDisposed() {
      return true;
    }

    @Override
    public Mono<Void> onClose() {
      return Mono.empty();
    }
  }

  /**
   * Wrapper of a RSocket, it computes statistics about the req/resp calls and update availability
   * accordingly.
   */
  private class WeightedSocket implements LoadBalancerSocketMetrics, RSocket {

    private static final double STARTUP_PENALTY = Long.MAX_VALUE >> 12;
    private final Quantile lowerQuantile;
    private final Quantile higherQuantile;
    private final long inactivityFactor;
    private final MonoProcessor<RSocket> rSocketMono;
    private volatile int pending; // instantaneous rate
    private long stamp; // last timestamp we sent a request
    private long stamp0; // last timestamp we sent a request or receive a response
    private long duration; // instantaneous cumulative duration

    private Median median;
    private Ewma interArrivalTime;

    private AtomicLong pendingStreams; // number of active streams

    private volatile double availability = 0.0;
    private final MonoProcessor<Void> onClose = MonoProcessor.create();

    WeightedSocket(
        RSocketSupplier factory,
        Quantile lowerQuantile,
        Quantile higherQuantile,
        int inactivityFactor) {
      this.rSocketMono = MonoProcessor.create();
      this.lowerQuantile = lowerQuantile;
      this.higherQuantile = higherQuantile;
      this.inactivityFactor = inactivityFactor;
      long now = Clock.now();
      this.stamp = now;
      this.stamp0 = now;
      this.duration = 0L;
      this.pending = 0;
      this.median = new Median();
      this.interArrivalTime = new Ewma(1, TimeUnit.MINUTES, DEFAULT_INITIAL_INTER_ARRIVAL_TIME);
      this.pendingStreams = new AtomicLong();

      logger.debug("Creating WeightedSocket {} from factory {}", WeightedSocket.this, factory);

      WeightedSocket.this
          .onClose()
          .doFinally(
              s -> {
                pool.accept(factory);
                activeSockets.remove(WeightedSocket.this);
                logger.debug(
                    "Removed {} from factory {} from activeSockets", WeightedSocket.this, factory);
              })
          .subscribe();

      factory
          .get()
          .retryWhen(
              Retry.backoff(weightedSocketRetries, weightedSocketBackOff)
                  .maxBackoff(weightedSocketMaxBackOff))
          .doOnError(
              throwable -> {
                logger.error(
                    "error while connecting {} from factory {}",
                    WeightedSocket.this,
                    factory,
                    throwable);
                WeightedSocket.this.dispose();
              })
          .subscribe(
              rSocket -> {
                // When RSocket is closed, close the WeightedSocket
                rSocket
                    .onClose()
                    .doFinally(
                        signalType -> {
                          logger.info(
                              "RSocket {} from factory {} closed", WeightedSocket.this, factory);
                          WeightedSocket.this.dispose();
                        })
                    .subscribe();

                // When the factory is closed, close the RSocket
                factory
                    .onClose()
                    .doFinally(
                        signalType -> {
                          logger.info("Factory {} closed", factory);
                          rSocket.dispose();
                        })
                    .subscribe();

                // When the WeightedSocket is closed, close the RSocket
                WeightedSocket.this
                    .onClose()
                    .doFinally(
                        signalType -> {
                          logger.info(
                              "WeightedSocket {} from factory {} closed",
                              WeightedSocket.this,
                              factory);
                          rSocket.dispose();
                        })
                    .subscribe();

                /*synchronized (LoadBalancedRSocketMono.this) {
                  if (activeSockets.size() >= targetAperture) {
                    quickSlowestRS();
                    pendingSockets -= 1;
                  }
                }*/
                rSocketMono.onNext(rSocket);
                availability = 1.0;
                if (!WeightedSocket.this
                    .isDisposed()) { // May be already disposed because of retryBackoff delay
                  activeSockets.add(WeightedSocket.this);
                  logger.debug(
                      "Added WeightedSocket {} from factory {} to activeSockets",
                      WeightedSocket.this,
                      factory);
                }
              });
    }

    WeightedSocket(RSocketSupplier factory, Quantile lowerQuantile, Quantile higherQuantile) {
      this(factory, lowerQuantile, higherQuantile, DEFAULT_INTER_ARRIVAL_FACTOR);
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
      return rSocketMono.flatMap(
          source ->
              Mono.from(
                  subscriber ->
                      source
                          .requestResponse(payload)
                          .subscribe(
                              new LatencySubscriber<>(
                                  Operators.toCoreSubscriber(subscriber), this))));
    }

    @Override
    public Flux<Payload> requestStream(Payload payload) {

      return rSocketMono.flatMapMany(
          source ->
              Flux.from(
                  subscriber ->
                      source
                          .requestStream(payload)
                          .subscribe(
                              new CountingSubscriber<>(
                                  Operators.toCoreSubscriber(subscriber), this))));
    }

    @Override
    public Mono<Void> fireAndForget(Payload payload) {

      return rSocketMono.flatMap(
          source -> {
            return Mono.from(
                subscriber ->
                    source
                        .fireAndForget(payload)
                        .subscribe(
                            new CountingSubscriber<>(
                                Operators.toCoreSubscriber(subscriber), this)));
          });
    }

    @Override
    public Mono<Void> metadataPush(Payload payload) {
      return rSocketMono.flatMap(
          source -> {
            return Mono.from(
                subscriber ->
                    source
                        .metadataPush(payload)
                        .subscribe(
                            new CountingSubscriber<>(
                                Operators.toCoreSubscriber(subscriber), this)));
          });
    }

    @Override
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {

      return rSocketMono.flatMapMany(
          source ->
              Flux.from(
                  subscriber ->
                      source
                          .requestChannel(payloads)
                          .subscribe(
                              new CountingSubscriber<>(
                                  Operators.toCoreSubscriber(subscriber), this))));
    }

    synchronized double getPredictedLatency() {
      long now = Clock.now();
      long elapsed = Math.max(now - stamp, 1L);

      double weight;
      double prediction = median.estimation();

      if (prediction == 0.0) {
        if (pending == 0) {
          weight = 0.0; // first request
        } else {
          // subsequent requests while we don't have any history
          weight = STARTUP_PENALTY + pending;
        }
      } else if (pending == 0 && elapsed > inactivityFactor * interArrivalTime.value()) {
        // if we did't see any data for a while, we decay the prediction by inserting
        // artificial 0.0 into the median
        median.insert(0.0);
        weight = median.estimation();
      } else {
        double predicted = prediction * pending;
        double instant = instantaneous(now);

        if (predicted < instant) { // NB: (0.0 < 0.0) == false
          weight = instant / pending; // NB: pending never equal 0 here
        } else {
          // we are under the predictions
          weight = prediction;
        }
      }

      return weight;
    }

    int getPending() {
      return pending;
    }

    private synchronized long instantaneous(long now) {
      return duration + (now - stamp0) * pending;
    }

    private synchronized long incr() {
      long now = Clock.now();
      interArrivalTime.insert(now - stamp);
      duration += Math.max(0, now - stamp0) * pending;
      pending += 1;
      stamp = now;
      stamp0 = now;
      return now;
    }

    private synchronized long decr(long timestamp) {
      long now = Clock.now();
      duration += Math.max(0, now - stamp0) * pending - (now - timestamp);
      pending -= 1;
      stamp0 = now;
      return now;
    }

    private synchronized void observe(double rtt) {
      median.insert(rtt);
      lowerQuantile.insert(rtt);
      higherQuantile.insert(rtt);
    }

    @Override
    public double availability() {
      return availability;
    }

    @Override
    public void dispose() {
      onClose.onComplete();
    }

    @Override
    public boolean isDisposed() {
      return onClose.isDisposed();
    }

    @Override
    public Mono<Void> onClose() {
      return onClose;
    }

    @Override
    public String toString() {
      return "WeightedSocket("
          + "median="
          + median.estimation()
          + " quantile-low="
          + lowerQuantile.estimation()
          + " quantile-high="
          + higherQuantile.estimation()
          + " inter-arrival="
          + interArrivalTime.value()
          + " duration/pending="
          + (pending == 0 ? 0 : (double) duration / pending)
          + " pending="
          + pending
          + " availability= "
          + availability()
          + ")->";
    }

    @Override
    public double medianLatency() {
      return median.estimation();
    }

    @Override
    public double lowerQuantileLatency() {
      return lowerQuantile.estimation();
    }

    @Override
    public double higherQuantileLatency() {
      return higherQuantile.estimation();
    }

    @Override
    public double interArrivalTime() {
      return interArrivalTime.value();
    }

    @Override
    public int pending() {
      return pending;
    }

    @Override
    public long lastTimeUsedMillis() {
      return stamp0;
    }

    /**
     * Subscriber wrapper used for request/response interaction model, measure and collect latency
     * information.
     */
    private class LatencySubscriber<U> implements CoreSubscriber<U> {
      private final CoreSubscriber<U> child;
      private final WeightedSocket socket;
      private final AtomicBoolean done;
      private long start;

      LatencySubscriber(CoreSubscriber<U> child, WeightedSocket socket) {
        this.child = child;
        this.socket = socket;
        this.done = new AtomicBoolean(false);
      }

      @Override
      public Context currentContext() {
        return child.currentContext();
      }

      @Override
      public void onSubscribe(Subscription s) {
        start = incr();
        child.onSubscribe(
            new Subscription() {
              @Override
              public void request(long n) {
                s.request(n);
              }

              @Override
              public void cancel() {
                if (done.compareAndSet(false, true)) {
                  s.cancel();
                  decr(start);
                }
              }
            });
      }

      @Override
      public void onNext(U u) {
        child.onNext(u);
      }

      @Override
      public void onError(Throwable t) {
        if (done.compareAndSet(false, true)) {
          child.onError(t);
          long now = decr(start);
          if (t instanceof TransportException || t instanceof ClosedChannelException) {
            socket.dispose();
          } else if (t instanceof TimeoutException) {
            observe(now - start);
          }
        }
      }

      @Override
      public void onComplete() {
        if (done.compareAndSet(false, true)) {
          long now = decr(start);
          observe(now - start);
          child.onComplete();
        }
      }
    }

    /**
     * Subscriber wrapper used for stream like interaction model, it only counts the number of
     * active streams
     */
    private class CountingSubscriber<U> implements CoreSubscriber<U> {
      private final CoreSubscriber<U> child;
      private final WeightedSocket socket;

      CountingSubscriber(CoreSubscriber<U> child, WeightedSocket socket) {
        this.child = child;
        this.socket = socket;
      }

      @Override
      public Context currentContext() {
        return child.currentContext();
      }

      @Override
      public void onSubscribe(Subscription s) {
        socket.pendingStreams.incrementAndGet();
        child.onSubscribe(s);
      }

      @Override
      public void onNext(U u) {
        child.onNext(u);
      }

      @Override
      public void onError(Throwable t) {
        socket.pendingStreams.decrementAndGet();
        child.onError(t);
        if (t instanceof TransportException || t instanceof ClosedChannelException) {
          logger.debug("Disposing {} from activeSockets because of error {}", socket, t);
          socket.dispose();
        }
      }

      @Override
      public void onComplete() {
        socket.pendingStreams.decrementAndGet();
        child.onComplete();
      }
    }
  }
}
