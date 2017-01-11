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

import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Spectator;
import io.reactivesocket.events.ServerEventListener;
import io.reactivesocket.spectator.internal.ThreadLocalAdderCounter;

public class ServerEventListenerImpl extends EventListenerImpl implements ServerEventListener {

    private final ThreadLocalAdderCounter socketAccepted;

    public ServerEventListenerImpl(Registry registry, String monitorId) {
        super(registry, monitorId);
        socketAccepted = new ThreadLocalAdderCounter(registry, "socketAccepted", monitorId);
    }

    public ServerEventListenerImpl(String monitorId) {
        this(Spectator.globalRegistry(), monitorId);
    }

    @Override
    public void socketAccepted() {
        socketAccepted.increment();
    }
}
