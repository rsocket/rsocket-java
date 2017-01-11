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
import com.netflix.spectator.api.Spectator;
import io.reactivesocket.events.EventListener.RequestType;
import io.reactivesocket.util.Clock;

import java.util.concurrent.TimeUnit;

public class RequestStats {

    private final Stats requestSentStats;
    private final Stats requestReceivedStats;
    private final Stats responseSentStats;
    private final Stats responseReceivedStats;

    public RequestStats(Registry registry, RequestType requestType, String monitorId) {
        requestSentStats = new Stats(registry, requestType, monitorId, "request", "sent");
        requestReceivedStats = new Stats(registry, requestType, monitorId, "request", "received");
        responseSentStats = new Stats(registry, requestType, monitorId, "response", "sent");
        responseReceivedStats = new Stats(registry, requestType, monitorId, "response", "received");
    }

    public RequestStats(RequestType requestType, String monitorId) {
        this(Spectator.globalRegistry(), requestType, monitorId);
    }

    public void requestSendStart() {
        requestSentStats.start.increment();
    }

    public void requestReceivedStart() {
        requestReceivedStats.start.increment();
    }

    public void requestSendSuccess(long duration, TimeUnit timeUnit) {
        requestSentStats.success.increment();
        requestSentStats.successLatency.record(Clock.unit().convert(duration, timeUnit));
    }

    public void requestReceivedSuccess(long duration, TimeUnit timeUnit) {
        requestReceivedStats.success.increment();
        requestReceivedStats.successLatency.record(Clock.unit().convert(duration, timeUnit));
    }

    public void requestSendFailed(long duration, TimeUnit timeUnit) {
        requestSentStats.failure.increment();
        requestSentStats.failureLatency.record(Clock.unit().convert(duration, timeUnit));
    }

    public void requestReceivedFailed(long duration, TimeUnit timeUnit) {
        requestReceivedStats.failure.increment();
        requestReceivedStats.failureLatency.record(Clock.unit().convert(duration, timeUnit));
    }

    public void requestSendCancelled(long duration, TimeUnit timeUnit) {
        requestSentStats.cancel.increment();
        requestSentStats.cancelLatency.record(Clock.unit().convert(duration, timeUnit));
    }

    public void requestReceivedCancelled(long duration, TimeUnit timeUnit) {
        requestReceivedStats.cancel.increment();
        requestReceivedStats.cancelLatency.record(Clock.unit().convert(duration, timeUnit));
    }

    public void responseSendStart(long requestToResponseLatency, TimeUnit timeUnit) {
        responseSentStats.start.increment();
        responseSentStats.processLatency.record(Clock.unit().convert(requestToResponseLatency, timeUnit));
    }

    public void responseReceivedStart(long requestToResponseLatency, TimeUnit timeUnit) {
        responseReceivedStats.start.increment();
        responseReceivedStats.processLatency.record(Clock.unit().convert(requestToResponseLatency, timeUnit));
    }

    public void responseSendSuccess(long duration, TimeUnit timeUnit) {
        responseSentStats.success.increment();
        responseSentStats.successLatency.record(Clock.unit().convert(duration, timeUnit));
    }

    public void responseReceivedSuccess(long duration, TimeUnit timeUnit) {
        responseReceivedStats.success.increment();
        responseReceivedStats.successLatency.record(Clock.unit().convert(duration, timeUnit));
    }

    public void responseSendFailed(long duration, TimeUnit timeUnit) {
        responseSentStats.failure.increment();
        responseSentStats.failureLatency.record(Clock.unit().convert(duration, timeUnit));
    }

    public void responseReceivedFailed(long duration, TimeUnit timeUnit) {
        responseReceivedStats.failure.increment();
        responseReceivedStats.failureLatency.record(Clock.unit().convert(duration, timeUnit));
    }

    public void responseSendCancelled(long duration, TimeUnit timeUnit) {
        responseSentStats.cancel.increment();
        responseSentStats.cancelLatency.record(Clock.unit().convert(duration, timeUnit));
    }

    public void responseReceivedCancelled(long duration, TimeUnit timeUnit) {
        responseReceivedStats.cancel.increment();
        responseReceivedStats.cancelLatency.record(Clock.unit().convert(duration, timeUnit));
    }

    private static class Stats {

        private final ThreadLocalAdderCounter start;
        private final ThreadLocalAdderCounter success;
        private final ThreadLocalAdderCounter failure;
        private final ThreadLocalAdderCounter cancel;
        private final HdrHistogramPercentileTimer successLatency;
        private final HdrHistogramPercentileTimer failureLatency;
        private final HdrHistogramPercentileTimer cancelLatency;
        private final HdrHistogramPercentileTimer processLatency;

        public Stats(Registry registry, RequestType requestType, String monitorId, String namePrefix,
                     String direction) {
            start = new ThreadLocalAdderCounter(registry, namePrefix + "Start", monitorId,
                                                "requestType", requestType.name(), "direction", direction);
            success = new ThreadLocalAdderCounter(registry, namePrefix + "Success", monitorId,
                                                  "requestType", requestType.name(), "direction", direction);
            failure = new ThreadLocalAdderCounter(registry, namePrefix + "Failure", monitorId,
                                                  "requestType", requestType.name(), "direction", direction);
            cancel = new ThreadLocalAdderCounter(registry, namePrefix + "Cancel", monitorId,
                                                 "requestType", requestType.name(), "direction", direction);
            successLatency = new HdrHistogramPercentileTimer(registry, namePrefix + "Latency", monitorId,
                                                             "requestType", requestType.name(),
                                                             "direction", direction, "outcome", "success");
            failureLatency = new HdrHistogramPercentileTimer(registry, namePrefix + "Latency", monitorId,
                                                             "requestType", requestType.name(),
                                                             "direction", direction, "outcome", "failure");
            cancelLatency = new HdrHistogramPercentileTimer(registry, namePrefix + "Latency", monitorId,
                                                             "requestType", requestType.name(),
                                                             "direction", direction, "outcome", "cancel");
            processLatency = new HdrHistogramPercentileTimer(registry, namePrefix + "processingTime", monitorId,
                                                             "requestType", requestType.name(),
                                                             "direction", direction);
        }
    }
}
