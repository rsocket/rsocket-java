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

package io.reactivesocket.events;

import org.slf4j.event.Level;

import java.util.concurrent.TimeUnit;
import java.util.function.DoubleSupplier;

public class LoggingClientEventListener extends LoggingEventListener implements ClientEventListener {

    public LoggingClientEventListener(String name, Level logLevel) {
        super(name, logLevel);
    }

    @Override
    public void connectStart() {
        logIfEnabled(() -> name + ": connectStart");
    }

    @Override
    public void connectCompleted(DoubleSupplier socketAvailabilitySupplier, long duration, TimeUnit durationUnit) {
        logIfEnabled(() -> name + ": connectCompleted " + "socketAvailabilitySupplier = [" + socketAvailabilitySupplier
                           + "], duration = [" + duration + "], durationUnit = [" + durationUnit + ']');
    }

    @Override
    public void connectFailed(long duration, TimeUnit durationUnit, Throwable cause) {
        logIfEnabled(() -> name + ": connectFailed " + "duration = [" + duration + "], durationUnit = [" +
                           durationUnit + "], cause = [" + cause + ']');
    }

    @Override
    public void connectCancelled(long duration, TimeUnit durationUnit) {
        logIfEnabled(() -> name + ": connectCancelled " + "duration = [" + duration + "], durationUnit = [" +
                           durationUnit + ']');
    }
}
