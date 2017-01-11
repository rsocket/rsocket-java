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

import com.netflix.spectator.api.Registry;

import static io.reactivesocket.frame.ErrorFrameFlyweight.*;

public class ErrorStats {

    private final ThreadLocalAdderCounter connectionErrorSent;
    private final ThreadLocalAdderCounter connectionErrorReceived;
    private final ThreadLocalAdderCounter rejectedSent;
    private final ThreadLocalAdderCounter rejectedReceived;
    private final ThreadLocalAdderCounter setupFailed;
    private final ThreadLocalAdderCounter otherSent;
    private final ThreadLocalAdderCounter otherReceived;

    public ErrorStats(Registry registry, String monitorId) {
        connectionErrorSent = new ThreadLocalAdderCounter(registry, "connectionError", monitorId,
                                                          "direction", "sent");
        connectionErrorReceived = new ThreadLocalAdderCounter(registry, "connectionError", monitorId,
                                                              "direction", "received");
        rejectedSent = new ThreadLocalAdderCounter(registry, "rejects", monitorId,
                                                   "direction", "sent");
        rejectedReceived = new ThreadLocalAdderCounter(registry, "rejects", monitorId,
                                                       "direction", "received");
        setupFailed = new ThreadLocalAdderCounter(registry, "setupFailed", monitorId);
        otherSent = new ThreadLocalAdderCounter(registry, "otherErrors", monitorId,
                                                   "direction", "sent");
        otherReceived = new ThreadLocalAdderCounter(registry, "otherErrors", monitorId,
                                                       "direction", "received");
    }

    public void onErrorSent(int errorCode) {
        getCounterForError(errorCode, true).increment();
    }

    public void onErrorReceived(int errorCode) {
        getCounterForError(errorCode, false).increment();
    }

    private ThreadLocalAdderCounter getCounterForError(int errorCode, boolean sent) {
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
