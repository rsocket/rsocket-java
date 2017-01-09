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

package io.reactivesocket.client.events;

import io.reactivesocket.Availability;
import io.reactivesocket.events.LoggingClientEventListener;
import org.slf4j.event.Level;

import java.util.concurrent.TimeUnit;

public class LoggingLoadBalancingClientListener extends LoggingClientEventListener implements LoadBalancingClientListener {

    public LoggingLoadBalancingClientListener(String name, Level logLevel) {
        super(name, logLevel);
    }

    @Override
    public void socketAdded(Availability availability) {
        logIfEnabled(() -> name + ": socketAdded " + "availability = [" + availability + ']');
    }

    @Override
    public void socketRemoved(Availability availability) {
        logIfEnabled(() -> name + ": socketRemoved " + "availability = [" + availability + ']');
    }

    @Override
    public void serverAdded(Availability availability) {
        logIfEnabled(() -> name + ": serverAdded " + "availability = [" + availability + ']');
    }

    @Override
    public void serverRemoved(Availability availability) {
        logIfEnabled(() -> name + ": serverRemoved " + "availability = [" + availability + ']');
    }

    @Override
    public void apertureChanged(int oldAperture, int newAperture) {
        logIfEnabled(() -> name + ": apertureChanged " + "oldAperture = [" + oldAperture + "newAperture = ["
                           + newAperture + ']');
    }

    @Override
    public void socketRefreshPeriodChanged(long oldPeriod, long newPeriod, TimeUnit periodUnit) {
        logIfEnabled(() -> name + ": socketRefreshPeriodChanged " + "newPeriod = [" + newPeriod + "], periodUnit = ["
                           + periodUnit + ']');
    }

    @Override
    public void socketsRefreshStart() {
        logIfEnabled(() -> name + ": socketsRefreshStart");
    }

    @Override
    public void socketsRefreshCompleted(long duration, TimeUnit durationUnit) {
        logIfEnabled(() -> name + ": socketsRefreshCompleted " + "duration = [" + duration +
                           "], durationUnit = [" + durationUnit + ']');
    }
}
