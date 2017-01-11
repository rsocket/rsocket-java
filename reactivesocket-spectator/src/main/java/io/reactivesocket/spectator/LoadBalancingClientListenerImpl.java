/*
 * Copyright 2017 Netflix, Inc.
 * <p>
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *  <p>
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  <p>
 *  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations under the License.
 */

package io.reactivesocket.spectator;

import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Spectator;
import io.reactivesocket.Availability;
import io.reactivesocket.client.LoadBalancerSocketMetrics;
import io.reactivesocket.client.events.LoadBalancingClientListener;
import io.reactivesocket.spectator.internal.ThreadLocalAdderCounter;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class LoadBalancingClientListenerImpl extends ClientEventListenerImpl
        implements LoadBalancingClientListener {

    private final ConcurrentHashMap<Availability, SocketStats> sockets;
    private final ConcurrentHashMap<Availability, Availability> servers;
    private final ThreadLocalAdderCounter socketsAdded;
    private final ThreadLocalAdderCounter socketsRemoved;
    private final ThreadLocalAdderCounter serversAdded;
    private final ThreadLocalAdderCounter serversRemoved;
    private final ThreadLocalAdderCounter socketRefresh;
    private final Gauge aperture;
    private final Gauge socketRefreshPeriodMillis;
    private final Registry registry;
    private final String monitorId;

    public LoadBalancingClientListenerImpl(Registry registry, String monitorId) {
        super(registry, monitorId);
        this.registry = registry;
        this.monitorId = monitorId;
        sockets = new ConcurrentHashMap<>();
        servers = new ConcurrentHashMap<>();
        socketsAdded = new ThreadLocalAdderCounter(registry, "socketsAdded", monitorId);
        socketsRemoved = new ThreadLocalAdderCounter(registry, "socketsRemoved", monitorId);
        serversAdded = new ThreadLocalAdderCounter(registry, "serversAdded", monitorId);
        serversRemoved = new ThreadLocalAdderCounter(registry, "serversRemoved", monitorId);
        socketRefresh = new ThreadLocalAdderCounter(registry, "socketRefresh", monitorId);
        aperture = registry.gauge(registry.createId("aperture", "id", monitorId));
        socketRefreshPeriodMillis = registry.gauge(registry.createId("socketRefreshPeriodMillis",
                                                                     "id", monitorId));
    }

    public LoadBalancingClientListenerImpl(String monitorId) {
        this(Spectator.globalRegistry(), monitorId);
    }

    @Override
    public void socketAdded(Availability availability) {
        if (availability instanceof LoadBalancerSocketMetrics) {
            sockets.put(availability, new SocketStats((LoadBalancerSocketMetrics) availability));
        }
        socketsAdded.increment();
    }

    @Override
    public void socketRemoved(Availability availability) {
        sockets.remove(availability);
        socketsRemoved.increment();
    }

    @Override
    public void serverAdded(Availability availability) {
        servers.put(availability, availability);
        registry.gauge(registry.createId("availability", "id", monitorId, "entity", "server",
                                         "entityId", String.valueOf(availability.hashCode())),
                       availability, a -> a.availability());
        serversAdded.increment();
    }

    @Override
    public void serverRemoved(Availability availability) {
        servers.remove(availability);
        serversRemoved.increment();
    }

    @Override
    public void apertureChanged(int oldAperture, int newAperture) {
        aperture.set(newAperture);
    }

    @Override
    public void socketRefreshPeriodChanged(long oldPeriod, long newPeriod, TimeUnit periodUnit) {
        socketRefreshPeriodMillis.set(TimeUnit.MILLISECONDS.convert(newPeriod, periodUnit));
    }

    @Override
    public void socketsRefreshCompleted(long duration, TimeUnit durationUnit) {
        socketRefresh.increment();
    }

    private final class SocketStats {

        public SocketStats(LoadBalancerSocketMetrics metrics) {
            registry.gauge(registry.createId("availability", "id", monitorId, "entity", "socket",
                                             "entityId", String.valueOf(metrics.hashCode())),
                           metrics, m -> m.availability());
            registry.gauge(registry.createId("pendingRequests", "id", monitorId, "entity", "socket",
                                             "entityId", String.valueOf(metrics.hashCode())),
                           metrics, m -> m.pending());
            registry.gauge(registry.createId("higherQuantileLatency", "id", monitorId, "entity", "socket",
                                             "entityId", String.valueOf(metrics.hashCode())),
                           metrics, m -> m.higherQuantileLatency());
            registry.gauge(registry.createId("lowerQuantileLatency", "id", monitorId, "entity", "socket",
                                             "entityId", String.valueOf(metrics.hashCode())),
                           metrics, m -> m.lowerQuantileLatency());
            registry.gauge(registry.createId("interArrivalTime", "id", monitorId, "entity", "socket",
                                             "entityId", String.valueOf(metrics.hashCode())),
                           metrics, m -> m.interArrivalTime());
            registry.gauge(registry.createId("lastTimeUsedMillis", "id", monitorId, "entity", "socket",
                                             "entityId", String.valueOf(metrics.hashCode())),
                           metrics, m -> m.lastTimeUsedMillis());
        }
    }
}
