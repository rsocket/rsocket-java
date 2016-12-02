/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.reactivesocket.client;

import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.exceptions.NoAvailableReactiveSocketException;
import io.reactivesocket.exceptions.TimeoutException;
import io.reactivesocket.exceptions.TransportException;
import io.reactivesocket.reactivestreams.extensions.Px;
import io.reactivesocket.reactivestreams.extensions.internal.EmptySubject;
import io.reactivesocket.reactivestreams.extensions.internal.ValidatingSubscription;
import io.reactivesocket.stat.Ewma;
import io.reactivesocket.stat.FrugalQuantile;
import io.reactivesocket.stat.Median;
import io.reactivesocket.stat.Quantile;
import io.reactivesocket.util.Clock;
import io.reactivesocket.util.ReactiveSocketProxy;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This {@link ReactiveSocket} implementation will load balance the request across a
 * pool of children ReactiveSockets.
 * It estimates the load of each ReactiveSocket based on statistics collected.
 */
public class LoadBalancer implements ReactiveSocket {

    private static final Logger logger = LoggerFactory.getLogger(LoadBalancer .class);

    public static final double DEFAULT_EXP_FACTOR = 4.0;
    public static final double DEFAULT_LOWER_QUANTILE = 0.2;
    public static final double DEFAULT_HIGHER_QUANTILE = 0.8;
    public static final double DEFAULT_MIN_PENDING = 1.0;
    public static final double DEFAULT_MAX_PENDING = 2.0;
    public static final int DEFAULT_MIN_APERTURE = 3;
    public static final int DEFAULT_MAX_APERTURE = 100;
    public static final long DEFAULT_MAX_REFRESH_PERIOD_MS = TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);

    private static final long APERTURE_REFRESH_PERIOD = Clock.unit().convert(15, TimeUnit.SECONDS);
    private static final int EFFORT = 5;
    private static final long DEFAULT_INITIAL_INTER_ARRIVAL_TIME = Clock.unit().convert(1L, TimeUnit.SECONDS);
    private static final int DEFAULT_INTER_ARRIVAL_FACTOR = 500;

    private final double minPendings;
    private final double maxPendings;
    private final int minAperture;
    private final int maxAperture;
    private final long maxRefreshPeriod;

    private final double expFactor;
    private final Quantile lowerQuantile;
    private final Quantile higherQuantile;
    private Runnable readyCallback;

    private int pendingSockets;
    private final List<WeightedSocket> activeSockets;
    private final List<ReactiveSocketClient> activeFactories;
    private final FactoriesRefresher factoryRefresher;

    private final Ewma pendings;
    private volatile int targetAperture;
    private long lastApertureRefresh;
    private long refreshPeriod;
    private volatile long lastRefresh;
    private final EmptySubject closeSubject = new EmptySubject();

    /**
     *
     * @param factories the source (factories) of ReactiveSocket
     * @param expFactor how aggressive is the algorithm toward outliers. A higher
     *                  number means we send aggressively less traffic to a server
     *                  slightly slower.
     * @param lowQuantile the lower bound of the latency band of acceptable values.
     *                    Any server below that value will be aggressively favored.
     * @param highQuantile the higher bound of the latency band of acceptable values.
     *                     Any server above that value will be aggressively penalized.
     * @param minPendings The lower band of the average outstanding messages per server.
     * @param maxPendings The higher band of the average outstanding messages per server.
     * @param minAperture the minimum number of connections we want to maintain,
     *                    independently of the load.
     * @param maxAperture the maximum number of connections we want to maintain,
     *                    independently of the load.
     * @param maxRefreshPeriodMs the maximum time between two "refreshes" of the list of active
     *                           ReactiveSocket. This is at that time that the slowest
     *                           ReactiveSocket is closed. (unit is millisecond)
     */
    public LoadBalancer(
        Publisher<? extends Collection<ReactiveSocketClient>> factories,
        double expFactor,
        double lowQuantile,
        double highQuantile,
        double minPendings,
        double maxPendings,
        int minAperture,
        int maxAperture,
        long maxRefreshPeriodMs
    ) {
        this.expFactor = expFactor;
        this.lowerQuantile = new FrugalQuantile(lowQuantile);
        this.higherQuantile = new FrugalQuantile(highQuantile);

        this.activeSockets = new ArrayList<>(128);
        this.activeFactories = new ArrayList<>(128);
        this.pendingSockets = 0;
        this.factoryRefresher = new FactoriesRefresher();

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

        factories.subscribe(factoryRefresher);
    }

    public LoadBalancer(Publisher<? extends Collection<ReactiveSocketClient>> factories) {
        this(factories,
            DEFAULT_EXP_FACTOR,
            DEFAULT_LOWER_QUANTILE, DEFAULT_HIGHER_QUANTILE,
            DEFAULT_MIN_PENDING, DEFAULT_MAX_PENDING,
            DEFAULT_MIN_APERTURE, DEFAULT_MAX_APERTURE,
            DEFAULT_MAX_REFRESH_PERIOD_MS
        );
    }

    LoadBalancer(Publisher<? extends Collection<ReactiveSocketClient>> factories, Runnable readyCallback) {
        this(factories,
            DEFAULT_EXP_FACTOR,
            DEFAULT_LOWER_QUANTILE, DEFAULT_HIGHER_QUANTILE,
            DEFAULT_MIN_PENDING, DEFAULT_MAX_PENDING,
            DEFAULT_MIN_APERTURE, DEFAULT_MAX_APERTURE,
            DEFAULT_MAX_REFRESH_PERIOD_MS
        );
        this.readyCallback = readyCallback;
    }

    @Override
    public Publisher<Void> fireAndForget(Payload payload) {
        return subscriber -> select().fireAndForget(payload).subscribe(subscriber);
    }

    @Override
    public Publisher<Payload> requestResponse(Payload payload) {
        return subscriber -> select().requestResponse(payload).subscribe(subscriber);
    }

    @Override
    public Publisher<Payload> requestSubscription(Payload payload) {
        return subscriber -> select().requestSubscription(payload).subscribe(subscriber);
    }

    @Override
    public Publisher<Payload> requestStream(Payload payload) {
        return subscriber -> select().requestStream(payload).subscribe(subscriber);
    }

    @Override
    public Publisher<Void> metadataPush(Payload payload) {
        return subscriber -> select().metadataPush(payload).subscribe(subscriber);
    }

    @Override
    public Publisher<Payload> requestChannel(Publisher<Payload> payloads) {
        return subscriber -> select().requestChannel(payloads).subscribe(subscriber);
    }

    private synchronized void addSockets(int numberOfNewSocket) {
        int n = numberOfNewSocket;
        if (n > activeFactories.size()) {
            n = activeFactories.size();
            logger.debug("addSockets({}) restricted by the number of factories, i.e. addSockets({})",
                numberOfNewSocket, n);
        }

        Random rng = ThreadLocalRandom.current();
        while (n > 0) {
            int size = activeFactories.size();
            if (size == 1) {
                ReactiveSocketClient factory = activeFactories.get(0);
                if (factory.availability() > 0.0) {
                    activeFactories.remove(0);
                    pendingSockets++;
                    factory.connect().subscribe(new SocketAdder(factory));
                }
                break;
            }
            ReactiveSocketClient factory0 = null;
            ReactiveSocketClient factory1 = null;
            int i0 = 0;
            int i1 = 0;
            for (int i = 0; i < EFFORT; i++) {
                i0 = rng.nextInt(size);
                i1 = rng.nextInt(size - 1);
                if (i1 >= i0) {
                    i1++;
                }
                factory0 = activeFactories.get(i0);
                factory1 = activeFactories.get(i1);
                if (factory0.availability() > 0.0 && factory1.availability() > 0.0) {
                    break;
                }
            }

            if (factory0.availability() < factory1.availability()) {
                n--;
                pendingSockets++;
                // cheaper to permute activeFactories.get(i1) with the last item and remove the last
                // rather than doing a activeFactories.remove(i1)
                if (i1 < size - 1) {
                    activeFactories.set(i1, activeFactories.get(size - 1));
                }
                activeFactories.remove(size - 1);
                factory1.connect().subscribe(new SocketAdder(factory1));
            } else {
                n--;
                pendingSockets++;
                // c.f. above
                if (i0 < size - 1) {
                    activeFactories.set(i0, activeFactories.get(size - 1));
                }
                activeFactories.remove(size - 1);
                factory0.connect().subscribe(new SocketAdder(factory0));
            }
        }
    }

    private synchronized void refreshAperture() {
        int n = activeSockets.size();
        if (n == 0) {
            return;
        }

        double p = 0.0;
        for (WeightedSocket wrs: activeSockets) {
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
     * @param newValue new aperture value
     * @param now time of the change (for rate limiting purposes)
     */
    private void updateAperture(int newValue, long now) {
        int previous = targetAperture;
        targetAperture = newValue;
        targetAperture = Math.max(minAperture, targetAperture);
        int maxAperture = Math.min(this.maxAperture, activeSockets.size() + activeFactories.size());
        targetAperture = Math.min(maxAperture, targetAperture);
        lastApertureRefresh = now;
        pendings.reset((minPendings + maxPendings)/2);

        if (targetAperture != previous) {
            logger.debug("Current pending={}, new target={}, previous target={}",
                pendings.value(), targetAperture, previous);
        }
    }

    /**
     * Responsible for:
     * - refreshing the aperture
     * - asynchronously adding/removing reactive sockets to match targetAperture
     * - periodically add a new connection
     */
    private synchronized void refreshSockets() {
        refreshAperture();

        int n = pendingSockets + activeSockets.size();
        if (n < targetAperture && !activeFactories.isEmpty()) {
            logger.debug("aperture {} is below target {}, adding {} sockets",
                n, targetAperture, targetAperture - n);
            addSockets(targetAperture - n);
        } else if (targetAperture <  activeSockets.size()) {
            logger.debug("aperture {} is above target {}, quicking 1 socket",
                n, targetAperture);
            quickSlowestRS();
        }

        long now = Clock.now();
        if (now - lastRefresh < refreshPeriod) {
            return;
        }

        long prev = refreshPeriod;
        refreshPeriod = (long) Math.min(refreshPeriod * 1.5, maxRefreshPeriod);
        logger.debug("Bumping refresh period, {}->{}", prev/1000, refreshPeriod/1000);
        lastRefresh = now;
        addSockets(1);
    }

    private synchronized void quickSlowestRS() {
        if (activeSockets.size() <= 1) {
            return;
        }

        WeightedSocket slowest = null;
        double lowestAvailability = Double.MAX_VALUE;
        for (WeightedSocket socket: activeSockets) {
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
            removeSocket(slowest);
        }
    }

    private synchronized void removeSocket(WeightedSocket socket) {
        try {
            logger.debug("Removing socket: -> " + socket);
            activeSockets.remove(socket);
            activeFactories.add(socket.getFactory());
            socket.close();
        } catch (Exception e) {
            logger.warn("Exception while closing a ReactiveSocket", e);
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

    private synchronized ReactiveSocket select() {
        if (activeSockets.isEmpty()) {
            return FAILING_REACTIVE_SOCKET;
        }
        refreshSockets();

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
            if (i+1 == EFFORT && !activeFactories.isEmpty()) {
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
        double high = Math.max(higherQuantile.estimation(), low * 1.001); // ensure higherQuantile > lowerQuantile + .1%
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
        return "LoadBalancer(a:" + activeSockets.size()+ ", f: "
            + activeFactories.size()
            + ", avgPendings=" + pendings.value()
            + ", targetAperture=" + targetAperture
            + ", band=[" + lowerQuantile.estimation()
            + ", " + higherQuantile.estimation()
            + "])";
    }

    @Override
    public Publisher<Void> close() {
        return subscriber -> {
            subscriber.onSubscribe(ValidatingSubscription.empty(subscriber));

            synchronized (this) {
                factoryRefresher.close();
                activeFactories.clear();
                AtomicInteger n = new AtomicInteger(activeSockets.size());

                activeSockets.forEach(rs -> {
                    rs.close().subscribe(new Subscriber<Void>() {
                        @Override
                        public void onSubscribe(Subscription s) {
                            s.request(Long.MAX_VALUE);
                        }

                        @Override
                        public void onNext(Void aVoid) {}

                        @Override
                        public void onError(Throwable t) {
                            logger.warn("Exception while closing a ReactiveSocket", t);
                            onComplete();
                        }

                        @Override
                        public void onComplete() {
                            if (n.decrementAndGet() == 0) {
                                subscriber.onComplete();
                                closeSubject.onComplete();
                            }
                        }
                    });
                });
            }
        };
    }

    @Override
    public Publisher<Void> onClose() {
        return closeSubject;
    }

    /**
     * This subscriber role is to subscribe to the list of server identifier, and update the
     * factory list.
     */
    private class FactoriesRefresher implements Subscriber<Collection<ReactiveSocketClient>> {
        private Subscription subscription;

        @Override
        public void onSubscribe(Subscription subscription) {
            this.subscription = subscription;
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(Collection<ReactiveSocketClient> newFactories) {
            synchronized (LoadBalancer.this) {

                Set<ReactiveSocketClient> current =
                    new HashSet<>(activeFactories.size() + activeSockets.size());
                current.addAll(activeFactories);
                for (WeightedSocket socket: activeSockets) {
                    ReactiveSocketClient factory = socket.getFactory();
                    current.add(factory);
                }

                Set<ReactiveSocketClient> removed = new HashSet<>(current);
                removed.removeAll(newFactories);

                Set<ReactiveSocketClient> added = new HashSet<>(newFactories);
                added.removeAll(current);

                boolean changed = false;
                Iterator<WeightedSocket> it0 = activeSockets.iterator();
                while (it0.hasNext()) {
                    WeightedSocket socket = it0.next();
                    if (removed.contains(socket.getFactory())) {
                        it0.remove();
                        try {
                            changed = true;
                            socket.close();
                        } catch (Exception e) {
                            logger.warn("Exception while closing a ReactiveSocket", e);
                        }
                    }
                }
                Iterator<ReactiveSocketClient> it1 = activeFactories.iterator();
                while (it1.hasNext()) {
                    ReactiveSocketClient factory = it1.next();
                    if (removed.contains(factory)) {
                        it1.remove();
                        changed = true;
                    }
                }

                activeFactories.addAll(added);

                if (changed && logger.isDebugEnabled()) {
                    StringBuilder msgBuilder = new StringBuilder();
                    msgBuilder.append("\nUpdated active factories (size: " + activeFactories.size() + ")\n");
                    for (ReactiveSocketClient f : activeFactories) {
                        msgBuilder.append(" + ").append(f).append('\n');
                    }
                    msgBuilder.append("Active sockets:\n");
                    for (WeightedSocket socket: activeSockets) {
                        msgBuilder.append(" + ").append(socket).append('\n');
                    }
                    logger.debug(msgBuilder.toString());
                }
            }
            refreshSockets();
        }

        @Override
        public void onError(Throwable t) {
            // TODO: retry
            logger.error("Error refreshing ReactiveSocket factories. They would no longer be refreshed.", t);
        }

        @Override
        public void onComplete() {
            // TODO: retry
            logger.warn("ReactiveSocket factories source completed. They would no longer be refreshed.");
        }

        void close() {
            subscription.cancel();
        }
    }

    private class SocketAdder implements Subscriber<ReactiveSocket> {
        private final ReactiveSocketClient factory;

        private int errors = 0;

        private SocketAdder(ReactiveSocketClient factory) {
            this.factory = factory;
        }

        @Override
        public void onSubscribe(Subscription s) {
            s.request(1L);
        }

        @Override
        public void onNext(ReactiveSocket rs) {
            synchronized (LoadBalancer.this) {
                if (activeSockets.size() >= targetAperture) {
                    quickSlowestRS();
                }

                WeightedSocket weightedSocket = new WeightedSocket(rs, factory, lowerQuantile, higherQuantile);
                logger.debug("Adding new WeightedSocket {}", weightedSocket);

                activeSockets.add(weightedSocket);
                if (readyCallback != null) {
                    readyCallback.run();
                }
                pendingSockets -= 1;
            }
        }

        @Override
        public void onError(Throwable t) {
            logger.warn("Exception while subscribing to the ReactiveSocket source", t);
            synchronized (LoadBalancer.this) {
                pendingSockets -= 1;
                if (++errors < 5) {
                    activeFactories.add(factory);
                } else {
                    logger.warn("Exception count greater than 5, not re-adding factory {}", factory.toString());
                }
            }
        }

        @Override
        public void onComplete() {}
    }

    private static final FailingReactiveSocket FAILING_REACTIVE_SOCKET = new FailingReactiveSocket();

    /**
     * (Null Object Pattern)
     * This failing ReactiveSocket never succeed, it is useful for simplifying the code
     * when dealing with edge cases.
     */
    private static class FailingReactiveSocket implements ReactiveSocket {

        @SuppressWarnings("ThrowableInstanceNeverThrown")
        private static final NoAvailableReactiveSocketException NO_AVAILABLE_RS_EXCEPTION =
            new NoAvailableReactiveSocketException();

        private static final Publisher<Void> errorVoid = Px.error(NO_AVAILABLE_RS_EXCEPTION);
        private static final Publisher<Payload> errorPayload = Px.error(NO_AVAILABLE_RS_EXCEPTION);

        @Override
        public Publisher<Void> fireAndForget(Payload payload) {
            return errorVoid;
        }

        @Override
        public Publisher<Payload> requestResponse(Payload payload) {
            return errorPayload;
        }

        @Override
        public Publisher<Payload> requestStream(Payload payload) {
            return errorPayload;
        }

        @Override
        public Publisher<Payload> requestSubscription(Payload payload) {
            return errorPayload;
        }

        @Override
        public Publisher<Payload> requestChannel(Publisher<Payload> payloads) {
            return errorPayload;
        }

        @Override
        public Publisher<Void> metadataPush(Payload payload) {
            return errorVoid;
        }

        @Override
        public double availability() {
            return 0;
        }

        @Override
        public Publisher<Void> close() {
            return Px.empty();
        }

        @Override
        public Publisher<Void> onClose() {
            return Px.empty();
        }
    }

    /**
     * Wrapper of a ReactiveSocket, it computes statistics about the req/resp calls and
     * update availability accordingly.
     */
    private class WeightedSocket extends ReactiveSocketProxy {
        private static final double STARTUP_PENALTY = Long.MAX_VALUE >> 12;

        private final ReactiveSocket child;
        private ReactiveSocketClient factory;
        private final Quantile lowerQuantile;
        private final Quantile higherQuantile;
        private final long inactivityFactor;

        private volatile int pending;       // instantaneous rate
        private long stamp;                 // last timestamp we sent a request
        private long stamp0;                // last timestamp we sent a request or receive a response
        private long duration;              // instantaneous cumulative duration

        private Median median;
        private Ewma interArrivalTime;

        private AtomicLong pendingStreams;  // number of active streams

        WeightedSocket(
            ReactiveSocket child,
            ReactiveSocketClient factory,
            Quantile lowerQuantile,
            Quantile higherQuantile,
            int inactivityFactor
        ) {
            super(child);
            this.child = child;
            this.factory = factory;
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
        }

        WeightedSocket(
            ReactiveSocket child,
            ReactiveSocketClient factory,
            Quantile lowerQuantile,
            Quantile higherQuantile
        ) {
            this(child, factory, lowerQuantile, higherQuantile, DEFAULT_INTER_ARRIVAL_FACTOR);
        }

        @Override
        public Publisher<Payload> requestResponse(Payload payload) {
            return subscriber ->
                child.requestResponse(payload).subscribe(new LatencySubscriber<>(subscriber, this));
        }

        @Override
        public Publisher<Payload> requestStream(Payload payload) {
            return subscriber ->
                child.requestStream(payload).subscribe(new CountingSubscriber<>(subscriber, this));
        }

        @Override
        public Publisher<Payload> requestSubscription(Payload payload) {
            return subscriber ->
                child.requestSubscription(payload).subscribe(new CountingSubscriber<>(subscriber, this));
        }

        @Override
        public Publisher<Void> fireAndForget(Payload payload) {
            return subscriber ->
                child.fireAndForget(payload).subscribe(new CountingSubscriber<>(subscriber, this));
        }

        @Override
        public Publisher<Void> metadataPush(Payload payload) {
            return subscriber ->
                child.metadataPush(payload).subscribe(new CountingSubscriber<>(subscriber, this));
        }

        @Override
        public Publisher<Payload> requestChannel(Publisher<Payload> payloads) {
            return subscriber ->
                child.requestChannel(payloads).subscribe(new CountingSubscriber<>(subscriber, this));
        }

        ReactiveSocketClient getFactory() {
            return factory;
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
        public Publisher<Void> close() {
            return child.close();
        }

        @Override
        public String toString() {
            return "WeightedSocket("
                + "median=" + median.estimation()
                + " quantile-low=" + lowerQuantile.estimation()
                + " quantile-high=" + higherQuantile.estimation()
                + " inter-arrival=" + interArrivalTime.value()
                + " duration/pending=" + (pending == 0 ? 0 : (double)duration / pending)
                + " pending=" + pending
                + " availability= " + availability()
                + ")->" + child;
        }

        /**
         * Subscriber wrapper used for request/response interaction model, measure and collect
         * latency information.
         */
        private class LatencySubscriber<U> implements Subscriber<U> {
            private final Subscriber<U> child;
            private final WeightedSocket socket;
            private final AtomicBoolean done;
            private long start;

            LatencySubscriber(Subscriber<U> child, WeightedSocket socket) {
                this.child = child;
                this.socket = socket;
                this.done = new AtomicBoolean(false);
            }

            @Override
            public void onSubscribe(Subscription s) {
                start = incr();
                child.onSubscribe(new Subscription() {
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
                        removeSocket(socket);
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
        private class CountingSubscriber<U> implements Subscriber<U> {
            private final Subscriber<U> child;
            private final WeightedSocket socket;

            CountingSubscriber(Subscriber<U> child, WeightedSocket socket) {
                this.child = child;
                this.socket = socket;
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
                    removeSocket(socket);
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
