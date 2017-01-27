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

import static io.reactivesocket.frame.ErrorFrameFlyweight.*;
import static io.reactivesocket.spectator.internal.SpectatorUtil.*;

public class ErrorStats {

    private final Counter connectionErrorSent;
    private final Counter connectionErrorReceived;
    private final Counter rejectedSent;
    private final Counter rejectedReceived;
    private final Counter setupFailed;
    private final Counter otherSent;
    private final Counter otherReceived;

    public ErrorStats(Registry registry, String monitorId) {
        connectionErrorSent = registry.counter(createId(registry, "connectionError", monitorId, "direction", "sent"));
        connectionErrorReceived = registry.counter(createId(registry, "connectionError", monitorId, "direction", "received"));
        rejectedSent = registry.counter(createId(registry, "rejects", monitorId, "direction", "sent"));
        rejectedReceived = registry.counter(createId(registry, "rejects", monitorId, "direction", "received"));
        setupFailed = registry.counter(createId(registry, "setupFailed", monitorId));
        otherSent = registry.counter(createId(registry, "otherErrors", monitorId, "direction", "sent"));
        otherReceived = registry.counter(createId(registry, "otherErrors", monitorId, "direction", "received"));
    }

    public void onErrorSent(int errorCode) {
        getCounterForError(errorCode, true).increment();
    }

    public void onErrorReceived(int errorCode) {
        getCounterForError(errorCode, false).increment();
    }

    private Counter getCounterForError(int errorCode, boolean sent) {
        switch (errorCode) {
        case INVALID_SETUP:
            return setupFailed;
        case REJECTED_SETUP:
            return setupFailed;
        case UNSUPPORTED_SETUP:
            return setupFailed;
        case REJECTED:
            return sent ? rejectedSent : rejectedReceived;
        case CONNECTION_ERROR:
            return sent ? connectionErrorSent : connectionErrorReceived;
        default:
            return sent ? otherSent : otherReceived;
        }
    }
}
