/**
 * Copyright 2016 Netflix, Inc.
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
package io.reactivesocket.client;

import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.ReactiveSocketFactory;
import io.reactivesocket.client.stat.Median;
import io.reactivesocket.client.util.Clock;
import io.reactivesocket.client.exception.NoAvailableReactiveSocketException;
import io.reactivesocket.client.stat.Ewma;
import io.reactivesocket.exceptions.TransportException;
import io.reactivesocket.rx.Completable;
import io.reactivesocket.client.stat.FrugalQuantile;
import io.reactivesocket.client.stat.Quantile;
import io.reactivesocket.util.ReactiveSocketProxy;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.ClosedChannelException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * This {@link ReactiveSocket} implementation will load balance the request across a
 * pool of children ReactiveSockets.
 * It estimates the load of each ReactiveSocket based on statistics collected.
 */
public class LoadBalancer<T> implements ReactiveSocket {
    private static Logger logger = LoggerFactory.getLogger(LoadBalancer .class);

    private static final long APERTURE_REFRESH_PERIOD = Clock.unit().convert(15, TimeUnit.SECONDS);
    private static final int EFFORT = 5;

    private final double minPendings;
    private final double maxPendings;
    private final int minAperture;
    private final int maxAperture;
    private final long maxRefreshPeriod;

    private final double expFactor;
    private final Quantile lowerQuantile;
    private final Quantile higherQuantile;

    private int pendingSockets;
    private final List<WeightedSocket> activeSockets;
    private final Deque<ReactiveSocketFactory<T>> activeFactories;
    private final FactoriesRefresher factoryRefresher;

    private Ewma pendings;
    private volatile int targetAperture;
    private long lastApertureRefresh;
    private long refreshPeriod;
    private volatile long lastRefresh;

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
     * @param maxRefreshPeriod the maximum time between two "refreshes" of the list of active
     *                         ReactiveSocket. This is at that time that the slowest
     *                         ReactiveSocket is closed.
     * @param maxRefreshPeriodUnit Unit of the above period.
     */
    public LoadBalancer(
        Publisher<? extends Collection<ReactiveSocketFactory<T>>> factories,
        double expFactor,
        double lowQuantile,
        double highQuantile,
        double minPendings,
        double maxPendings,
        int minAperture,
        int maxAperture,
        long maxRefreshPeriod,
        TimeUnit maxRefreshPeriodUnit
    ) {
        this.expFactor = expFactor;
        this.lowerQuantile = new FrugalQuantile(lowQuantile);
        this.higherQuantile = new FrugalQuantile(highQuantile);

        this.activeSockets = new ArrayList<>();
        this.activeFactories = new ArrayDeque<>();
        this.pendingSockets = 0;
        this.factoryRefresher = new FactoriesRefresher();

        this.minPendings = minPendings;
        this.maxPendings = maxPendings;
        this.pendings = new Ewma(15, TimeUnit.SECONDS, (minPendings + maxPendings) / 2.0);

        this.minAperture = minAperture;
        this.maxAperture = maxAperture;
        this.targetAperture = minAperture;

        this.maxRefreshPeriod = Clock.unit().convert(maxRefreshPeriod, maxRefreshPeriodUnit);
        this.lastApertureRefresh = Clock.now();
        this.refreshPeriod = Clock.unit().convert(15L, TimeUnit.SECONDS);
        this.lastRefresh = Clock.now();

        factories.subscribe(factoryRefresher);
    }

    public LoadBalancer(Publisher<? extends Collection<ReactiveSocketFactory<T>>> factories) {
        this(factories,
            4.0,
            0.2, 0.8,
            1.0, 2.0,
            3, 100,
            5, TimeUnit.MINUTES
        );
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
        int n = Math.min(activeFactories.size(), numberOfNewSocket);
        int maxEffort = 1 + activeFactories.size()/2;
        int effort = 0;
        while (n > 0 && effort < maxEffort) {
            ReactiveSocketFactory<T> factory0 = activeFactories.poll();
            ReactiveSocketFactory<T> factory1 = activeFactories.poll();
            if (factory0 == null) {
                // queue is empty
                break;
            }
            if (factory1 == null) {
                // factory0 was the last item in the queue
                if (factory0.availability() > 0.0) {
                    pendingSockets++;
                    factory0.apply().subscribe(new SocketAdder(factory0));
                } else {
                    activeFactories.addLast(factory0);
                }
                break;
            }

            if (factory0.availability() == 0.0 && factory1.availability() == 0.0) {
                activeFactories.addLast(factory0);
                activeFactories.addLast(factory1);
                effort++;
            } else if (factory0.availability() < factory1.availability()) {
                n--;
                pendingSockets++;
                activeFactories.addLast(factory0);
                factory1.apply().subscribe(new SocketAdder(factory1));
            } else {
                n--;
                pendingSockets++;
                activeFactories.addLast(factory1);
                factory0.apply().subscribe(new SocketAdder(factory0));
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
        p /= (n + pendingSockets);
        pendings.insert(p);
        double avgPending = pendings.value();

        long now = Clock.now();
        boolean underRateLimit = now - lastApertureRefresh > APERTURE_REFRESH_PERIOD;
        int previous = targetAperture;
        if (avgPending < 1.0 && underRateLimit) {
            targetAperture--;
            lastApertureRefresh = now;
            pendings.reset((minPendings + maxPendings)/2);
        } else if (2.0 < avgPending && underRateLimit) {
            targetAperture++;
            lastApertureRefresh = now;
            pendings.reset((minPendings + maxPendings)/2);
        }
        targetAperture = Math.max(minAperture, targetAperture);
        int maxAperture = Math.min(this.maxAperture, activeSockets.size() + activeFactories.size());
        targetAperture = Math.min(maxAperture, targetAperture);

        if (targetAperture != previous) {
            logger.info("Current pending=" + avgPending
                + ", new target=" + targetAperture
                + ", previous target=" + previous);
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
        if (n < targetAperture) {
            logger.info("aperture " + n
                + " is below target " + targetAperture
                + ", adding " + (targetAperture - n) + " sockets");
            addSockets(targetAperture - n);
        } else if (targetAperture < n) {
            logger.info("aperture " + n
                + " is above target " + targetAperture
                + ", quicking 1 socket");
            quickSlowestRS();
        }

        long now = Clock.now();
        if (now - lastRefresh < refreshPeriod) {
            return;
        } else {
            long prev = refreshPeriod;
            refreshPeriod = (long) Math.min(refreshPeriod * 1.5, maxRefreshPeriod);
            logger.info("Bumping refresh period, " + (prev/1000) + "->" + (refreshPeriod/1000));
        }
        lastRefresh = now;
        addSockets(1);
    }

    private synchronized void quickSlowestRS() {
        if (activeSockets.size() <= 1) {
            return;
        }

        activeSockets.forEach(value -> {
            logger.info("> " + value);
        });

        WeightedSocket slowest = null;
        double lowestAvailability = Double.MAX_VALUE;
        for (WeightedSocket socket: activeSockets) {
            double load = socket.availability();
            if (socket.getPredictedLatency() != 0) {
                load *= 1.0 / socket.getPredictedLatency();
            }
            if (load < lowestAvailability) {
                lowestAvailability = load;
                slowest = socket;
                if (load == 0.0) {
                    break;
                }
            }
        }

        if (slowest != null) {
            removeSocket(slowest);
        }
    }

    private synchronized void removeSocket(WeightedSocket socket) {
        try {
            logger.info("Removing socket: -> " + socket);
            activeSockets.remove(socket);
            activeFactories.addLast(socket.getFactory());
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

    @Override
    public void start(Completable c) {
        c.success(); // automatically started in the constructor
    }

    @Override
    public void onRequestReady(Consumer<Throwable> c) {
        throw new RuntimeException("onRequestReady not implemented");
    }

    @Override
    public void onRequestReady(Completable c) {
        throw new RuntimeException("onRequestReady not implemented");
    }

    @Override
    public void onShutdown(Completable c) {
        throw new RuntimeException("onShutdown not implemented");
    }

    @Override
    public synchronized void sendLease(int ttl, int numberOfRequests) {
        activeSockets.forEach(socket ->
            socket.sendLease(ttl, numberOfRequests)
        );
    }

    @Override
    public void shutdown() {
        try {
            close();
        } catch (Exception e) {
            logger.warn("Exception while calling `shutdown` on a ReactiveSocket", e);
        }
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
            if (rsc1.availability() > 0.0 && rsc2.availability() > 0.0)
                break;
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
    public synchronized void close() throws Exception {
        // TODO: have a `closed` flag?
        factoryRefresher.close();
        activeFactories.clear();
        activeSockets.forEach(rs -> {
            try {
                rs.close();
            } catch (Exception e) {
                logger.warn("Exception while closing a ReactiveSocket", e);
            }
        });
    }

    /**
     * This subscriber role is to subscribe to the list of server identifier, and update the
     * factory list.
     */
    private class FactoriesRefresher implements Subscriber<Collection<ReactiveSocketFactory<T>>> {
        private Subscription subscription;

        @Override
        public void onSubscribe(Subscription subscription) {
            this.subscription = subscription;
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(Collection<ReactiveSocketFactory<T>> newFactories) {
            List<ReactiveSocketFactory<T>> removed = computeRemoved(newFactories);
            synchronized (LoadBalancer.this) {
                boolean changed = false;
                for (ReactiveSocketFactory<T> factory : removed) {
                    activeFactories.remove(factory);

                    Iterator<WeightedSocket> it = activeSockets.iterator();
                    while (it.hasNext()) {
                        WeightedSocket socket = it.next();
                        if (socket.getFactory() == factory) {
                            it.remove();
                            try {
                                changed = true;
                                socket.close();
                            } catch (Exception e) {
                                logger.warn("Exception while closing a ReactiveSocket", e);
                            }
                        }
                    }
                }
                changed = changed || activeFactories.addAll(newFactories);

                if (changed && logger.isInfoEnabled()) {
                    String msg = "Updating active factories\n";
                    for (ReactiveSocketFactory<T> f : activeFactories) {
                        msg += " + " + f + "\n";
                    }
                    logger.info(msg);
                }
            }
            refreshSockets();
        }

        @Override
        public void onError(Throwable t) {
            // TODO: retry
        }

        @Override
        public void onComplete() {
            // TODO: retry
        }

        void close() {
            subscription.cancel();
        }

        private List<ReactiveSocketFactory<T>> computeRemoved(Collection<ReactiveSocketFactory<T>> newFactories) {
            ArrayList<ReactiveSocketFactory<T>> removed = new ArrayList<>();

            synchronized (LoadBalancer.this) {
                for (ReactiveSocketFactory<T> factory : activeFactories) {
                    boolean isRemoved = true;
                    for (ReactiveSocketFactory<T> f : newFactories) {
                        if (f.remote() == factory.remote()) {
                            isRemoved = false;
                            break;
                        }
                    }
                    if (isRemoved) {
                        removed.add(factory);
                    }
                }
            }
            return removed;
        }
    }

    private class SocketAdder implements Subscriber<ReactiveSocket> {
        private final ReactiveSocketFactory<T> factory;

        private SocketAdder(ReactiveSocketFactory<T> factory) {
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
                logger.info("Adding new WeightedSocket "
                    + weightedSocket + " connected to " + factory.remote());

                activeSockets.add(weightedSocket);
                pendingSockets -= 1;
            }
        }

        @Override
        public void onError(Throwable t) {
            logger.warn("Exception while subscribing to the ReactiveSocket source", t);
            synchronized (LoadBalancer.this) {
                pendingSockets -= 1;
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

        @Override
        public Publisher<Void> fireAndForget(Payload payload) {
            return subscriber -> subscriber.onError(NO_AVAILABLE_RS_EXCEPTION);
        }

        @Override
        public Publisher<Payload> requestResponse(Payload payload) {
            return subscriber -> subscriber.onError(NO_AVAILABLE_RS_EXCEPTION);
        }

        @Override
        public Publisher<Payload> requestStream(Payload payload) {
            return subscriber -> subscriber.onError(NO_AVAILABLE_RS_EXCEPTION);
        }

        @Override
        public Publisher<Payload> requestSubscription(Payload payload) {
            return subscriber -> subscriber.onError(NO_AVAILABLE_RS_EXCEPTION);
        }

        @Override
        public Publisher<Payload> requestChannel(Publisher<Payload> payloads) {
            return subscriber -> subscriber.onError(NO_AVAILABLE_RS_EXCEPTION);
        }

        @Override
        public Publisher<Void> metadataPush(Payload payload) {
            return subscriber -> subscriber.onError(NO_AVAILABLE_RS_EXCEPTION);
        }

        @Override
        public double availability() {
            return 0;
        }

        @Override
        public void start(Completable c) {
            c.error(NO_AVAILABLE_RS_EXCEPTION);
        }

        @Override
        public void onRequestReady(Consumer<Throwable> c) {
            c.accept(NO_AVAILABLE_RS_EXCEPTION);
        }

        @Override
        public void onRequestReady(Completable c) {
            c.error(NO_AVAILABLE_RS_EXCEPTION);
        }

        @Override
        public void onShutdown(Completable c) {
            c.error(NO_AVAILABLE_RS_EXCEPTION);
        }

        @Override
        public void sendLease(int ttl, int numberOfRequests) {}

        @Override
        public void shutdown() {}

        @Override
        public void close() throws Exception {}
    }

    /**
     * Wrapper of a ReactiveSocket, it computes statistics about the req/resp calls and
     * update availability accordingly.
     */
    private class WeightedSocket extends ReactiveSocketProxy {
        private static final double STARTUP_PENALTY = Long.MAX_VALUE >> 12;

        private final ReactiveSocket child;
        private ReactiveSocketFactory<T> factory;
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
            ReactiveSocketFactory<T> factory,
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
            this.interArrivalTime = new Ewma(1, TimeUnit.MINUTES, 1000);
            this.pendingStreams = new AtomicLong();
        }

        WeightedSocket(
            ReactiveSocket child,
            ReactiveSocketFactory<T> factory,
            Quantile lowerQuantile,
            Quantile higherQuantile
        ) {
            this(child, factory, lowerQuantile, higherQuantile, 100);
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

        ReactiveSocketFactory<T> getFactory() {
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
        public void close() throws Exception {
            child.close();
        }

        @Override
        public String toString() {
            return "WeightedSocket@" + hashCode()
                + " [median:" + median.estimation()
                + " quantile-low:" + lowerQuantile.estimation()
                + " quantile-high:" + higherQuantile.estimation()
                + " inter-arrival:" + interArrivalTime.value()
                + " duration/pending:" + (pending == 0 ? 0 : (double)duration / pending)
                + " availability: " + availability()
                + "]->" + child.toString();
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
                pendingStreams.incrementAndGet();
                child.onSubscribe(s);
            }

            @Override
            public void onNext(U u) {
                child.onNext(u);
            }

            @Override
            public void onError(Throwable t) {
                pendingStreams.decrementAndGet();
                child.onError(t);
                removeSocket(socket);
            }

            @Override
            public void onComplete() {
                pendingStreams.decrementAndGet();
                child.onComplete();
            }
        }
    }
}
