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

package io.reactivesocket.spectator.internal;

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;

import static io.reactivesocket.spectator.internal.SpectatorUtil.*;

public class LeaseStats {

    private final Counter leaseSent;
    private final Counter ttlSent;
    private final Counter leaseReceived;
    private final Counter ttlReceived;

    public LeaseStats(Registry registry, String monitorId, String... tags) {
        leaseSent = registry.counter(createId(registry, "lease", monitorId, mergeTags(tags, "direction", "sent")));
        ttlSent = registry.counter(createId(registry, "ttl", monitorId, mergeTags(tags, "direction", "sent")));
        leaseReceived = registry.counter(createId(registry, "lease", monitorId, mergeTags(tags, "direction", "received")));
        ttlReceived = registry.counter(createId(registry, "ttl", monitorId, mergeTags(tags, "direction", "received")));
    }

    public void newLeaseSent(int permits, int ttl) {
        leaseSent.increment(permits);
        ttlSent.increment(ttl);
    }

    public void newLeaseReceived(int permits, int ttl) {
        leaseReceived.increment(permits);
        ttlReceived.increment(ttl);
    }
}
