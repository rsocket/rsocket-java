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

package io.reactivesocket.events;

import io.reactivesocket.DuplexConnection;
import io.reactivesocket.FrameType;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.events.EventSource.EventSubscription;
import io.reactivesocket.lease.Lease;

import java.util.concurrent.TimeUnit;

/**
 * A listener of events for {@link ReactiveSocket}
 */
public interface EventListener {

    /**
     * An enum to represent the various interaction models of {@code ReactiveSocket}.
     */
    enum RequestType {
        RequestResponse,
        RequestStream,
        RequestChannel,
        MetadataPush,
        FireAndForget;

        public static RequestType fromFrameType(FrameType frameType) {
            switch (frameType) {
            case REQUEST_RESPONSE:
                return RequestResponse;
            case FIRE_AND_FORGET:
                return FireAndForget;
            case REQUEST_STREAM:
                return RequestStream;
            case REQUEST_CHANNEL:
                return RequestChannel;
            case METADATA_PUSH:
                return MetadataPush;
            default:
                throw new IllegalArgumentException("Unknown frame type: " + frameType);
            }
        }
    }

    /**
     * Start event for receiving a new request from the peer. This callback will be invoked when the first frame for the
     * request is received.
     *
     * @param streamId Stream Id for the request.
     * @param type Request type.
     */
    default void requestReceiveStart(int streamId, RequestType type) {}

    /**
     * End event for receiving a new request from the peer. This callback will be invoked when the last frame for the
     * request is received. For single item requests like {@link ReactiveSocket#requestResponse(Payload)}, the two
     * events {@link #requestReceiveStart(int, RequestType)} and this will be emitted for the same frame. In case
     * request ends with an error, {@link #requestReceiveFailed(int, RequestType, long, TimeUnit, Throwable)} will be
     * called instead.
     *
     * @param streamId Stream Id for the request.
     * @param type Request type.
     * @param duration Time in the {@code durationUnit} since the start of the request receive.
     * @param durationUnit {@code TimeUnit} for the duration.
     */
    default void requestReceiveComplete(int streamId, RequestType type, long duration, TimeUnit durationUnit) {}

    /**
     * End event for receiving a new request from the peer. This callback will be invoked when an error frame is
     * received for the request. If the request is successfully completed,
     * {@link #requestReceiveComplete(int, RequestType, long, TimeUnit)} will be called instead.
     *
     * @param streamId Stream Id for the request.
     * @param type Request type.
     * @param duration Time in the {@code durationUnit} since the start of the request receive.
     * @param durationUnit {@code TimeUnit} for the duration.
     * @param cause Cause for the failure.
     */
    default void requestReceiveFailed(int streamId, RequestType type, long duration, TimeUnit durationUnit,
                                      Throwable cause) {}

    /**
     * Cancel event for receiving a new request from the peer. This callback will be invoked when request receive is
     * cancelled.
     *
     * @param streamId Stream Id for the request.
     * @param type Request type.
     * @param duration Time in the {@code durationUnit} since the start of the request receive.
     * @param durationUnit {@code TimeUnit} for the duration.
     */
    default void requestReceiveCancelled(int streamId, RequestType type, long duration, TimeUnit durationUnit) {}

    /**
     * Start event for sending a new request to the peer. This callback will be invoked when first frame of the
     * request is successfully written to the underlying {@link DuplexConnection}. <p>
     *     For latencies related to write and buffering of frames, the events must be exposed by the transport.
     *
     * @param streamId Stream Id for the request.
     * @param type Request type.
     */
    default void requestSendStart(int streamId, RequestType type) {}

    /**
     * End event for sending a new request to the peer. This callback will be invoked when last frame of the
     * request is successfully written to the underlying {@link DuplexConnection}.
     *
     * @param streamId Stream Id for the request.
     * @param type Request type.
     * @param duration Time between subscription to request stream and last.
     * @param durationUnit {@code TimeUnit} for the duration.
     */
    default void requestSendComplete(int streamId, RequestType type, long duration, TimeUnit durationUnit) {}

    /**
     * End event for sending a new request to the peer. This callback will be invoked if the request itself emits an
     * error or the write to the underlying {@link DuplexConnection} failed.
     *
     * @param streamId Stream Id for the request.
     * @param type Request type.
     * @param duration Time between subscription to request stream and error.
     * @param durationUnit {@code TimeUnit} for the duration.
     * @param cause Cause for the failure.
     */
    default void requestSendFailed(int streamId, RequestType type, long duration, TimeUnit durationUnit,
                                   Throwable cause) {}

    /**
     * Cancel event for sending a new request to the peer. This callback will be invoked if the write was cancelled by
     * transport or user cancelled the response before the request was written.
     *
     * @param streamId Stream Id for the request.
     * @param type Request type.
     * @param duration Time between subscription to request stream and cancellation.
     * @param durationUnit {@code TimeUnit} for the duration.
     */
    default void requestSendCancelled(int streamId, RequestType type, long duration, TimeUnit durationUnit) {
    }

    /**
     * Start event for sending a response to the peer. This callback will be invoked when first frame of the
     * response is written to the underlying {@link DuplexConnection}.
     *
     * @param streamId Stream Id for the response.
     * @param type Request type.
     * @param duration Time between event {@link #requestReceiveComplete(int, RequestType, long, TimeUnit)} and this.
     * @param durationUnit {@code TimeUnit} for the duration.
     */
    default void responseSendStart(int streamId, RequestType type, long duration, TimeUnit durationUnit) {}

    /**
     * End event for sending a response to the peer. This callback will be invoked when last frame of the
     * response is written to the underlying {@link DuplexConnection}.
     *
     * @param streamId Stream Id for the response.
     * @param type Request type.
     * @param duration Time between subscription to response stream and last.
     * @param durationUnit {@code TimeUnit} for the duration.
     */
    default void responseSendComplete(int streamId, RequestType type, long duration, TimeUnit durationUnit) {}

    /**
     * End event for sending a response to the peer. This callback will be invoked when the response terminates with
     * an error.
     *
     * @param streamId Stream Id for the response.
     * @param type Request type.
     * @param duration Time between subscription to response stream and error.
     * @param durationUnit {@code TimeUnit} for the duration.
     * @param cause Cause for the failure.
     */
    default void responseSendFailed(int streamId, RequestType type, long duration, TimeUnit durationUnit,
                                       Throwable cause) {}

    /**
     * Cancel event for sending a response to the peer. This callback will be invoked if the write was cancelled by
     * transport or peer cancelled the response subscription.
     *
     * @param streamId Stream Id for the response.
     * @param type Request type.
     * @param duration Time between subscription to response stream and cancel.
     * @param durationUnit {@code TimeUnit} for the duration.
     */
    default void responseSendCancelled(int streamId, RequestType type, long duration, TimeUnit durationUnit) {
    }

    /**
     * Start event for receiving a response from the peer. This callback will be invoked when first frame of the
     * response is received from the underlying {@link DuplexConnection}.
     *
     * @param streamId Stream Id for the response.
     * @param type Request type.
     * @param duration Time between event {@link #requestSendComplete(int, RequestType, long, TimeUnit)} and this.
     * @param durationUnit {@code TimeUnit} for the duration.
     */
    default void responseReceiveStart(int streamId, RequestType type, long duration, TimeUnit durationUnit) {}

    /**
     * End event for receiving a response from the peer. This callback will be invoked when last frame of the
     * response is received from the underlying {@link DuplexConnection}.
     *
     * @param streamId Stream Id for the response.
     * @param type Request type.
     * @param duration Time between subscription to response stream and completion.
     * @param durationUnit {@code TimeUnit} for the duration.
     */
    default void responseReceiveComplete(int streamId, RequestType type, long duration, TimeUnit durationUnit) {}

    /**
     * End event for receiving a response from the peer. This callback will be invoked when the response terminates with
     * an error.
     *
     * @param streamId Stream Id for the response.
     * @param type Request type.
     * @param duration Time between subscription to response stream and error.
     * @param durationUnit {@code TimeUnit} for the duration.
     * @param cause Cause for the failure.
     */
    default void responseReceiveFailed(int streamId, RequestType type, long duration, TimeUnit durationUnit,
                                       Throwable cause) {}

    /**
     * Cancel event for receiving a response from the peer. This callback will be invoked if the user cancelled the
     * response subscription.
     *
     * @param streamId Stream Id for the response.
     * @param type Request type.
     * @param duration Time between subscription to response stream and error.
     * @param durationUnit {@code TimeUnit} for the duration.
     */
    default void responseReceiveCancelled(int streamId, RequestType type, long duration, TimeUnit durationUnit) {
    }

    /**
     * On {@code ReactiveSocket} close.
     *
     * @param duration Time for which the socket was active.
     * @param durationUnit {@code TimeUnit} for the duration.
     */
    default void socketClosed(long duration, TimeUnit durationUnit) {}

    /**
     * When a frame of type {@code frameType} is written.
     *
     * @param streamId Stream Id for the frame.
     * @param frameType Type of the frame.
     */
    default void frameWritten(int streamId, FrameType frameType) {}

    /**
     * When a frame of type {@code frameType} is read.
     *
     * @param streamId Stream Id for the frame.
     * @param frameType Type of the frame.
     */
    default void frameRead(int streamId, FrameType frameType) {}

    /**
     * When a lease is sent.
     *
     * @param permits Permits in the lease.
     * @param ttl Time to live for the lease.
     */
    default void leaseSent(int permits, int ttl) {}

    /**
     * When a lease is received.
     *
     * @param permits Permits in the lease.
     * @param ttl Time to live for the lease.
     */
    default void leaseReceived(int permits, int ttl) {}

    /**
     * When an error is sent.
     *
     * @param streamId Stream Id for the error.
     * @param errorCode Error code.
     */
    default void errorSent(int streamId, int errorCode) {}

    /**
     * When an error is received.
     *
     * @param streamId Stream Id for the error.
     * @param errorCode Error code.
     */
    default void errorReceived(int streamId, int errorCode) {}

    /**
     * Disposes this listener. This is a callback that can be invoked as a result of {@link EventSubscription#cancel()}
     * of the associated subscription OR as an explicit signal from the {@link EventSource}.<p>
     *     This would mark the end of notifications to this listener. Some in-flight notifications may be sent, due to
     *     the concurrent nature of the act of disposing and generation of notifications.
     */
    default void dispose() { }
}
