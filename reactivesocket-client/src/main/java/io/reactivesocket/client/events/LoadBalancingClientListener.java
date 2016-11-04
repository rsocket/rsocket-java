/*
 * Copyright 2016 Netflix, Inc.
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

package io.reactivesocket.client.events;

import io.reactivesocket.client.LoadBalancingClient;
import io.reactivesocket.events.ClientEventListener;

import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * A {@link ClientEventListener} for {@link LoadBalancingClient}
 */
public interface LoadBalancingClientListener extends ClientEventListener {

    /**
     * Event when a new socket is added to the load balancer.
     *
     * @param socketAddress Address for the socket.
     */
    default void socketAdded(SocketAddress socketAddress) {}

    /**
     * Event when a socket is removed from the load balancer.
     *
     * @param socketAddress Address for the socket.
     */
    default void socketRemoved(SocketAddress socketAddress) {}

    /**
     * An event when a server is added to the load balancer.
     *
     * @param socketAddress Address for the server.
     */
    default void serverAdded(SocketAddress socketAddress) {}

    /**
     * An event when a server is removed from the load balancer.
     *
     * @param socketAddress Address for the server.
     */
    default void serverRemoved(SocketAddress socketAddress) {}

    /**
     * An event when the expected number of active sockets held by the load balancer changes.
     *
     * @param newAperture New aperture size, i.e. expected number of active sockets.
     */
    default void apertureChanged(int newAperture) {}

    /**
     * An event when the expected time period for refreshing active sockets in the load balancer changes.
     *
     * @param newPeriod New refresh period.
     * @param periodUnit {@link TimeUnit} for the refresh period.
     */
    default void socketRefreshPeriodChanged(long newPeriod, TimeUnit periodUnit) {}

    /**
     * An event to mark the start of the socket refresh cycle.
     */
    default void socketsRefreshStart() {}

    /**
     * An event to mark the end of the socket refresh cycle.
     *
     * @param duration Time taken to refresh sockets.
     * @param durationUnit {@code TimeUnit} for the duration.
     */
    default void socketsRefreshCompleted(long duration, TimeUnit durationUnit) {}
}
