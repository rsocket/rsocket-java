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

import io.reactivesocket.FrameType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class LoggingEventListener implements EventListener {

    private final Logger logger;

    protected final String name;
    protected final Level logLevel;

    public LoggingEventListener(String name, Level logLevel) {
        this.name = name;
        this.logLevel = logLevel;
        logger = LoggerFactory.getLogger(name);
    }

    @Override
    public void requestReceiveStart(int streamId, RequestType type) {
        logIfEnabled(() -> name + ": requestReceiveStart " + "streamId = [" + streamId + "], type = [" + type + ']');
    }

    @Override
    public void requestReceiveComplete(int streamId, RequestType type, long duration, TimeUnit durationUnit) {
        logIfEnabled(() -> name + ": requestReceiveComplete " + "streamId = [" + streamId + "], type = [" + type +
                           "], duration = [" + duration + "], durationUnit = [" + durationUnit + ']');
    }

    @Override
    public void requestReceiveFailed(int streamId, RequestType type, long duration, TimeUnit durationUnit,
                                     Throwable cause) {
        logIfEnabled(() -> name + ": requestReceiveFailed " + "streamId = [" + streamId + "], type = [" + type +
                           "], duration = [" + duration + "], durationUnit = [" + durationUnit + "], cause = ["
                           + cause + ']');
    }

    @Override
    public void requestReceiveCancelled(int streamId, RequestType type, long duration, TimeUnit durationUnit) {
        logIfEnabled(() -> name + ": requestReceiveCancelled " + "streamId = [" + streamId + "], type = [" + type +
                           "], duration = [" + duration + "], durationUnit = [" + durationUnit + ']');
    }

    @Override
    public void requestSendStart(int streamId, RequestType type) {
        logIfEnabled(() -> name + ": requestSendStart " + "streamId = [" + streamId + "], type = [" + type + ']');
    }

    @Override
    public void requestSendComplete(int streamId, RequestType type, long duration, TimeUnit durationUnit) {
        logIfEnabled(() -> name + ": requestSendComplete " + "streamId = [" + streamId + "], type = [" + type +
                           "], duration = [" + duration + "], durationUnit = [" + durationUnit + ']');
    }

    @Override
    public void requestSendFailed(int streamId, RequestType type, long duration, TimeUnit durationUnit,
                                  Throwable cause) {
        logIfEnabled(() -> name + ": requestSendFailed " + "streamId = [" + streamId + "], type = [" + type +
                           "], duration = [" + duration + "], durationUnit = [" + durationUnit + "], cause = [" +
                           cause + ']');
    }

    @Override
    public void requestSendCancelled(int streamId, RequestType type, long duration, TimeUnit durationUnit) {
        logIfEnabled(() -> name + ": requestSendCancelled " + "streamId = [" + streamId + "], type = [" + type +
                           "], duration = [" + duration + "], durationUnit = [" + durationUnit + ']');
    }

    @Override
    public void responseSendStart(int streamId, RequestType type, long duration, TimeUnit durationUnit) {
        logIfEnabled(() -> name + ": responseSendStart " + "streamId = [" + streamId + "], type = [" + type +
                           "], duration = [" + duration + "], durationUnit = [" + durationUnit + ']');
    }

    @Override
    public void responseSendComplete(int streamId, RequestType type, long duration, TimeUnit durationUnit) {
        logIfEnabled(() -> name + ": responseSendComplete " + "streamId = [" + streamId + "], type = [" + type +
                           "], duration = [" + duration + "], durationUnit = [" + durationUnit + ']');
    }

    @Override
    public void responseSendFailed(int streamId, RequestType type, long duration, TimeUnit durationUnit,
                                   Throwable cause) {
        System.out.println(name + ": responseSendFailed " + "streamId = [" + streamId + "], type = [" + type +
                           "], duration = [" + duration + "], durationUnit = [" + durationUnit + "], cause = [" +
                           cause + ']');
    }

    @Override
    public void responseSendCancelled(int streamId, RequestType type, long duration, TimeUnit durationUnit) {
        logIfEnabled(() -> name + ": responseSendCancelled " + "streamId = [" + streamId + "], type = [" + type +
                           "], duration = [" + duration + "], durationUnit = [" + durationUnit + ']');
    }

    @Override
    public void responseReceiveStart(int streamId, RequestType type, long duration, TimeUnit durationUnit) {
        logIfEnabled(() -> name + ": responseReceiveStart " + "streamId = [" + streamId + "], type = [" + type +
                           "], duration = [" + duration + "], durationUnit = [" + durationUnit + ']');
    }

    @Override
    public void responseReceiveComplete(int streamId, RequestType type, long duration, TimeUnit durationUnit) {
        logIfEnabled(() -> name + ": responseReceiveComplete " + "streamId = [" + streamId + "], type = [" + type +
                           "], duration = [" + duration + "], durationUnit = [" + durationUnit + ']');
    }

    @Override
    public void responseReceiveFailed(int streamId, RequestType type, long duration, TimeUnit durationUnit,
                                      Throwable cause) {
        logIfEnabled(() -> name + ": responseReceiveFailed " + "streamId = [" + streamId + "], type = [" + type +
                           "], duration = [" + duration + "], durationUnit = [" + durationUnit + "], cause = [" +
                           cause + ']');
    }

    @Override
    public void responseReceiveCancelled(int streamId, RequestType type, long duration, TimeUnit durationUnit) {
        logIfEnabled(() -> name + ": responseReceiveCancelled " + "streamId = [" + streamId + "], type = [" + type +
                           "], duration = [" + duration + "], durationUnit = [" + durationUnit + ']');
    }

    @Override
    public void socketClosed(long duration, TimeUnit durationUnit) {
        logIfEnabled(() -> name + ": socketClosed " + "duration = [" + duration + "], durationUnit = [" +
                           durationUnit + ']');
    }

    @Override
    public void frameWritten(int streamId, FrameType frameType) {
        logIfEnabled(() -> name + ": frameWritten " + "streamId = [" + streamId + "], frameType = [" + frameType + ']');
    }

    @Override
    public void frameRead(int streamId, FrameType frameType) {
        logIfEnabled(() -> name + ": frameRead " + "streamId = [" + streamId + "], frameType = [" + frameType + ']');
    }

    @Override
    public void leaseSent(int permits, int ttl) {
        logIfEnabled(() -> name + ": leaseSent " + "permits = [" + permits + "], ttl = [" + ttl + ']');
    }

    @Override
    public void leaseReceived(int permits, int ttl) {
        logIfEnabled(() -> name + ": leaseReceived " + "permits = [" + permits + "], ttl = [" + ttl + ']');
    }

    @Override
    public void errorSent(int streamId, int errorCode) {
        logIfEnabled(() -> name + ": errorSent " + "streamId = [" + streamId + "], errorCode = [" + errorCode + ']');
    }

    @Override
    public void errorReceived(int streamId, int errorCode) {
        logIfEnabled(() -> name + ": errorReceived " + "streamId = [" + streamId + "], errorCode = [" + errorCode + ']');
    }

    @Override
    public void dispose() {
        logIfEnabled(() -> name + ": dispose");
    }

    protected void logIfEnabled(Supplier<String> logMsgSupplier) {
        switch (logLevel) {
        case ERROR:
            if (logger.isErrorEnabled()) {
                logger.error(logMsgSupplier.get());
            }
            break;
        case WARN:
            if (logger.isWarnEnabled()) {
                logger.warn(logMsgSupplier.get());
            }
            break;
        case INFO:
            if (logger.isInfoEnabled()) {
                logger.info(logMsgSupplier.get());
            }
            break;
        case DEBUG:
            if (logger.isDebugEnabled()) {
                logger.debug(logMsgSupplier.get());
            }
            break;
        case TRACE:
            if (logger.isTraceEnabled()) {
                logger.trace(logMsgSupplier.get());
            }
            break;
        }
    }
}
