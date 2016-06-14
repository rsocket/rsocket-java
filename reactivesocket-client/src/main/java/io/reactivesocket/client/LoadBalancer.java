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
import io.reactivesocket.exceptions.TransportException;
import io.reactivesocket.client.util.Clock;
import io.reactivesocket.client.exception.NoAvailableReactiveSocketException;
import io.reactivesocket.client.stat.Ewma;
import io.reactivesocket.rx.Completable;
import io.reactivesocket.client.stat.FrugalQuantile;
import io.reactivesocket.client.stat.Quantile;
import io.reactivesocket.util.ReactiveSocketProxy;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * This {@link ReactiveSocket} implementation will load balance the request across a
 * pool of children ReactiveSockets.
 * It estimates the load of each ReactiveSocket based on statistics collected.
 */
public class LoadBalancer implements ReactiveSocket {
    private static Logger logger = LoggerFactory.getLogger(LoadBalancer .class);

    private static final double MIN_PENDINGS = 1.0;
    private static final double MAX_PENDINGS = 2.0;
    private static final int MIN_APERTURE = 3;
    private static final int MAX_APERTURE = 100;
    private static final long APERTURE_REFRESH_PERIOD = Clock.unit().convert(15, TimeUnit.SECONDS);
    private static final long MAX_REFRESH_PERIOD = Clock.unit().convert(5, TimeUnit.MINUTES);
    private static final int EFFORT = 5;

    private final double expFactor;
    private final Quantile lowerQuantile;
    private final Quantile higherQuantile;

    private int pendingSockets;
    private final Map<SocketAddress, WeightedSocket> activeSockets;
    private final Map<SocketAddress, ReactiveSocketFactory<SocketAddress>> activeFactories;
    private final FactoriesRefresher factoryRefresher;

    private Ewma pendings;
    private volatile int targetAperture;
    private long lastApertureRefresh;
    private long refreshPeriod;
    private volatile long lastRefresh;

    public LoadBalancer(Publisher<List<ReactiveSocketFactory<SocketAddress>>> factories) {
        this.expFactor = 4.0;
        this.lowerQuantile = new FrugalQuantile(0.2);
        this.higherQuantile = new FrugalQuantile(0.8);

        this.activeSockets = new HashMap<>();
        this.activeFactories = new HashMap<>();
        this.pendingSockets = 0;
        this.factoryRefresher = new FactoriesRefresher();

        this.pendings = new Ewma(15, TimeUnit.SECONDS, (MIN_PENDINGS + MAX_PENDINGS) / 2);
        this.targetAperture = MIN_APERTURE;
        this.lastApertureRefresh = 0L;
        this.refreshPeriod = Clock.unit().convert(15, TimeUnit.SECONDS);
        this.lastRefresh = Clock.now();

        factories.subscribe(factoryRefresher);
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
        // TODO: deal with subscription & cie
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
        activeFactories.entrySet()
            .stream()
            // available factories that don't map to an already established socket
            .filter(e -> !activeSockets.containsKey(e.getKey()))
            .map(e -> e.getValue())
            .filter(factory -> factory.availability() > 0.0)
            .sorted((a, b) -> -Double.compare(a.availability(), b.availability()))
            .limit(numberOfNewSocket)
            .forEach(factory -> {
                pendingSockets += 1;
                factory.apply().subscribe(new SocketAdder(factory.remote()));
            });
    }

    private synchronized void refreshAperture() {
        int n = activeSockets.size();
        if (n == 0) {
            return;
        }

        double p = 0.0;
        for (WeightedSocket wrs: activeSockets.values()) {
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
            pendings.reset((MIN_PENDINGS + MAX_PENDINGS)/2);
        } else if (2.0 < avgPending && underRateLimit) {
            targetAperture++;
            lastApertureRefresh = now;
            pendings.reset((MIN_PENDINGS + MAX_PENDINGS)/2);
        }
        targetAperture = Math.max(MIN_APERTURE, targetAperture);
        int maxAperture = Math.min(MAX_APERTURE, activeFactories.size());
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
            refreshPeriod = (long) Math.min(refreshPeriod * 1.5, MAX_REFRESH_PERIOD);
            logger.info("Bumping refresh period, " + (prev/1000) + "->" + (refreshPeriod/1000));
        }
        lastRefresh = now;
        addSockets(1);
    }

    private synchronized void quickSlowestRS() {
        if (activeSockets.size() <= 1) {
            return;
        }

        activeSockets.entrySet().forEach(e -> {
            SocketAddress key = e.getKey();
            WeightedSocket value = e.getValue();
            logger.info("> " + key + " -> " + value);
        });

        activeSockets.entrySet()
            .stream()
            .sorted((a,b) -> {
                WeightedSocket socket1 = a.getValue();
                WeightedSocket socket2 = b.getValue();
                double load1 = 1.0/socket1.getPredictedLatency() * socket1.availability();
                double load2 = 1.0/socket2.getPredictedLatency() * socket2.availability();
                return Double.compare(load1, load2);
            })
            .limit(1)
            .forEach(entry -> {
                SocketAddress key = entry.getKey();
                WeightedSocket slowest = entry.getValue();
                try {
                    logger.info("quicking slowest: " + key + " -> " + slowest);
                    activeSockets.remove(key);
                    slowest.close();
                } catch (Exception e) {
                    logger.warn("Exception while closing a ReactiveSocket", e);
                }
            });
    }

    @Override
    public synchronized double availability() {
        double currentAvailability = 0.0;
        if (!activeSockets.isEmpty()) {
            for (WeightedSocket rs : activeSockets.values()) {
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
        activeSockets.values().forEach(socket ->
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
        List<WeightedSocket> buffer = activeSockets.values().stream().collect(Collectors.toList());
        if (size == 1) {
            return buffer.get(0);
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
            rsc1 = buffer.get(i1);
            rsc2 = buffer.get(i2);
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
        activeSockets.values().forEach(rs -> {
            try {
                rs.close();
            } catch (Exception e) {
                logger.warn("Exception while closing a ReactiveSocket", e);
            }
        });
    }

    private class RemoveItselfSubscriber implements Subscriber<Payload> {
        private Subscriber<? super Payload> child;
        private SocketAddress key;

        private RemoveItselfSubscriber(Subscriber<? super Payload> child, SocketAddress key) {
            this.child = child;
            this.key = key;
        }

        @Override
        public void onSubscribe(Subscription s) {
            child.onSubscribe(s);
        }

        @Override
        public void onNext(Payload payload) {
            child.onNext(payload);
        }

        @Override
        public void onError(Throwable t) {
            child.onError(t);
            if (t instanceof TransportException) {
                System.out.println(t + " removing socket " + child);
                synchronized (LoadBalancer.this) {
                    activeSockets.remove(key);
                }
            }
        }

        @Override
        public void onComplete() {
            child.onComplete();
        }
    }

    /**
     * This subscriber role is to subscribe to the list of server identifier, and update the
     * factory list.
     */
    private class FactoriesRefresher implements Subscriber<List<ReactiveSocketFactory<SocketAddress>>> {
        private Subscription subscription;

        @Override
        public void onSubscribe(Subscription subscription) {
            this.subscription = subscription;
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(List<ReactiveSocketFactory<SocketAddress>> newFactories) {
            List<ReactiveSocketFactory<SocketAddress>> removed = computeRemoved(newFactories);
            synchronized (LoadBalancer.this) {
                boolean changed = false;
                for (ReactiveSocketFactory<SocketAddress> factory : removed) {
                    SocketAddress key = factory.remote();
                    activeFactories.remove(key);
                    WeightedSocket removedSocket = activeSockets.remove(key);
                    try {
                        if (removedSocket != null) {
                            changed = true;
                            removedSocket.close();
                        }
                    } catch (Exception e) {
                        logger.warn("Exception while closing a ReactiveSocket", e);
                    }
                }

                for (ReactiveSocketFactory<SocketAddress> factory : newFactories) {
                    if (!activeFactories.containsKey(factory.remote())) {
                        activeFactories.put(factory.remote(), factory);
                        changed = true;
                    }
                }

                if (changed && logger.isInfoEnabled()) {
                    String msg = "UPDATING ACTIVE FACTORIES";
                    for (Map.Entry<SocketAddress, ReactiveSocketFactory<SocketAddress>> e : activeFactories.entrySet()) {
                        msg += " + " + e.getKey() + ": " + e.getValue() + "\n";
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

        private List<ReactiveSocketFactory<SocketAddress>> computeRemoved(
            List<ReactiveSocketFactory<SocketAddress>> newFactories) {
            ArrayList<ReactiveSocketFactory<SocketAddress>> removed = new ArrayList<>();

            synchronized (LoadBalancer.this) {
                for (Map.Entry<SocketAddress, ReactiveSocketFactory<SocketAddress>> e : activeFactories.entrySet()) {
                    SocketAddress key = e.getKey();
                    ReactiveSocketFactory<SocketAddress> factory = e.getValue();

                    boolean isRemoved = true;
                    for (ReactiveSocketFactory<SocketAddress> f : newFactories) {
                        if (f.remote() == key) {
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
        private final SocketAddress remote;

        private SocketAdder(SocketAddress remote) {
            this.remote = remote;
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

                ReactiveSocket proxy = new ReactiveSocketProxy(rs,
                    s -> new RemoveItselfSubscriber(s, remote));
                WeightedSocket weightedSocket = new WeightedSocket(proxy, lowerQuantile, higherQuantile);
                logger.info("Adding new WeightedSocket "
                    + weightedSocket + " connected to " + remote);
                activeSockets.put(remote, weightedSocket);
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
}
