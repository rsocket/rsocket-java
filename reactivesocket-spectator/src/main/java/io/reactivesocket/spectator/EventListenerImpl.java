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
import io.reactivesocket.FrameType;
import io.reactivesocket.events.EventListener;
import io.reactivesocket.spectator.internal.ErrorStats;
import io.reactivesocket.spectator.internal.LeaseStats;
import io.reactivesocket.spectator.internal.RequestStats;
import io.reactivesocket.spectator.internal.ThreadLocalAdderCounter;

import java.util.EnumMap;
import java.util.concurrent.TimeUnit;

public class EventListenerImpl implements EventListener {

    private final LeaseStats leaseStats;
    private final ErrorStats errorStats;
    private final ThreadLocalAdderCounter socketClosed;
    private final ThreadLocalAdderCounter frameRead;
    private final ThreadLocalAdderCounter frameWritten;
    private final EnumMap<RequestType, RequestStats> requestStats;

    public EventListenerImpl(Registry registry, String monitorId) {
        leaseStats = new LeaseStats(registry, monitorId);
        errorStats = new ErrorStats(registry, monitorId);
        socketClosed = new ThreadLocalAdderCounter(registry, "socketClosed", monitorId);
        frameRead = new ThreadLocalAdderCounter(registry, "frameRead", monitorId);
        frameWritten = new ThreadLocalAdderCounter(registry, "frameWritten", monitorId);
        requestStats = new EnumMap<RequestType, RequestStats>(RequestType.class);
        for (RequestType type : RequestType.values()) {
            requestStats.put(type, new RequestStats(registry, type, monitorId));
        }
    }

    public EventListenerImpl(String monitorId) {
        this(Spectator.globalRegistry(), monitorId);
    }

    @Override
    public void requestReceiveStart(int streamId, RequestType type) {
        requestStats.get(type).requestReceivedStart();
    }

    @Override
    public void requestReceiveComplete(int streamId, RequestType type, long duration, TimeUnit durationUnit) {
        requestStats.get(type).requestReceivedSuccess(duration, durationUnit);
    }

    @Override
    public void requestReceiveFailed(int streamId, RequestType type, long duration, TimeUnit durationUnit,
                                     Throwable cause) {
        requestStats.get(type).requestReceivedFailed(duration, durationUnit);
    }

    @Override
    public void requestReceiveCancelled(int streamId, RequestType type, long duration, TimeUnit durationUnit) {
        requestStats.get(type).requestReceivedCancelled(duration, durationUnit);

    }

    @Override
    public void requestSendStart(int streamId, RequestType type) {
        requestStats.get(type).requestSendStart();
    }

    @Override
    public void requestSendComplete(int streamId, RequestType type, long duration, TimeUnit durationUnit) {
        requestStats.get(type).requestSendSuccess(duration, durationUnit);
    }

    @Override
    public void requestSendFailed(int streamId, RequestType type, long duration, TimeUnit durationUnit,
                                  Throwable cause) {
        requestStats.get(type).requestSendFailed(duration, durationUnit);
    }

    @Override
    public void requestSendCancelled(int streamId, RequestType type, long duration, TimeUnit durationUnit) {
        requestStats.get(type).requestSendCancelled(duration, durationUnit);
    }

    @Override
    public void responseSendStart(int streamId, RequestType type, long duration, TimeUnit durationUnit) {
        requestStats.get(type).responseSendStart(duration, durationUnit);
    }

    @Override
    public void responseSendComplete(int streamId, RequestType type, long duration, TimeUnit durationUnit) {
        requestStats.get(type).responseSendSuccess(duration, durationUnit);
    }

    @Override
    public void responseSendFailed(int streamId, RequestType type, long duration, TimeUnit durationUnit,
                                   Throwable cause) {
        requestStats.get(type).responseSendFailed(duration, durationUnit);
    }

    @Override
    public void responseSendCancelled(int streamId, RequestType type, long duration, TimeUnit durationUnit) {
        requestStats.get(type).responseSendCancelled(duration, durationUnit);
    }

    @Override
    public void responseReceiveStart(int streamId, RequestType type, long duration, TimeUnit durationUnit) {
        requestStats.get(type).responseReceivedStart(duration, durationUnit);
    }

    @Override
    public void responseReceiveComplete(int streamId, RequestType type, long duration, TimeUnit durationUnit) {
        requestStats.get(type).responseReceivedSuccess(duration, durationUnit);
    }

    @Override
    public void responseReceiveFailed(int streamId, RequestType type, long duration, TimeUnit durationUnit,
                                      Throwable cause) {
        requestStats.get(type).responseReceivedFailed(duration, durationUnit);
    }

    @Override
    public void responseReceiveCancelled(int streamId, RequestType type, long duration, TimeUnit durationUnit) {
        requestStats.get(type).responseReceivedCancelled(duration, durationUnit);
    }

    @Override
    public void socketClosed(long duration, TimeUnit durationUnit) {
        socketClosed.increment();
    }

    @Override
    public void frameWritten(int streamId, FrameType frameType) {
        frameWritten.increment();
    }

    @Override
    public void frameRead(int streamId, FrameType frameType) {
        frameRead.increment();
    }

    @Override
    public void leaseSent(int permits, int ttl) {
        leaseStats.newLeaseSent(permits, ttl);
    }

    @Override
    public void leaseReceived(int permits, int ttl) {
        leaseStats.newLeaseReceived(permits, ttl);
    }

    @Override
    public void errorSent(int streamId, int errorCode) {
        errorStats.onErrorSent(errorCode);
    }

    @Override
    public void errorReceived(int streamId, int errorCode) {
        errorStats.onErrorReceived(errorCode);
    }
}
