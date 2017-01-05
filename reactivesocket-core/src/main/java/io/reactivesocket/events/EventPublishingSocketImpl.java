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

import io.reactivesocket.events.EventListener.RequestType;
import io.reactivesocket.internal.EventPublisher;
import io.reactivesocket.reactivestreams.extensions.internal.publishers.InstrumentingPublisher;
import io.reactivesocket.util.Clock;
import org.reactivestreams.Publisher;

import java.util.concurrent.TimeUnit;

public class EventPublishingSocketImpl implements EventPublishingSocket {

    private final EventPublisher<? extends EventListener> eventPublisher;
    private final boolean client;

    public EventPublishingSocketImpl(EventPublisher<? extends EventListener> eventPublisher, boolean client) {
        this.eventPublisher = eventPublisher;
        this.client = client;
    }

    @Override
    public <T> Publisher<T> decorateReceive(int streamId, Publisher<T> stream, RequestType requestType) {
        final long startTime = Clock.now();
        return new InstrumentingPublisher<>(stream,
                                            subscriber -> new ReceiveInterceptor(streamId, requestType, startTime),
                                            ReceiveInterceptor::receiveFailed, ReceiveInterceptor::receiveComplete,
                                            ReceiveInterceptor::receiveCancelled, null);
    }

    @Override
    public <T> Publisher<T> decorateSend(int streamId, Publisher<T> stream, long receiveStartTimeNanos,
                                         RequestType requestType) {
        return new InstrumentingPublisher<>(stream,
                                            subscriber -> new SendInterceptor(streamId, requestType,
                                                                              receiveStartTimeNanos),
                                            SendInterceptor::sendFailed, SendInterceptor::sendComplete,
                                            SendInterceptor::sendCancelled, null);
    }

    private class ReceiveInterceptor {

        private final long startTime;
        private final RequestType requestType;
        private final int streamId;

        public ReceiveInterceptor(int streamId, RequestType requestType, long startTime) {
            this.streamId = streamId;
            this.startTime = startTime;
            this.requestType = requestType;
            if (eventPublisher.isEventPublishingEnabled()) {
                EventListener eventListener = eventPublisher.getEventListener();
                if (client) {
                    eventListener.responseReceiveStart(streamId, requestType, Clock.elapsedSince(startTime),
                                                       Clock.unit());
                } else {
                    eventListener.requestReceiveStart(streamId, requestType);
                }
            }
        }

        public void receiveComplete() {
            if (eventPublisher.isEventPublishingEnabled()) {
                EventListener eventListener = eventPublisher.getEventListener();
                if (client) {
                    eventListener
                            .responseReceiveComplete(streamId, requestType, Clock.elapsedSince(startTime),
                                                     Clock.unit());
                } else {
                    eventListener
                            .requestReceiveComplete(streamId, requestType, Clock.elapsedSince(startTime), Clock.unit());
                }
            }
        }

        public void receiveFailed(Throwable cause) {
            if (eventPublisher.isEventPublishingEnabled()) {
                EventListener eventListener = eventPublisher.getEventListener();
                if (client) {
                    eventListener.responseReceiveFailed(streamId, requestType,
                                                        Clock.elapsedSince(startTime), Clock.unit(), cause);
                } else {
                    eventListener.requestReceiveFailed(streamId, requestType,
                                                       Clock.elapsedSince(startTime), Clock.unit(), cause);
                }
            }
        }

        public void receiveCancelled() {
            if (eventPublisher.isEventPublishingEnabled()) {
                EventListener eventListener = eventPublisher.getEventListener();
                if (client) {
                    eventListener.responseReceiveCancelled(streamId, requestType,
                                                           Clock.elapsedSince(startTime), Clock.unit());
                } else {
                    eventListener.requestReceiveCancelled(streamId, requestType,
                                                          Clock.elapsedSince(startTime), Clock.unit());
                }
            }
        }
    }

    private class SendInterceptor {

        private final long startTime;
        private final RequestType requestType;
        private final int streamId;

        public SendInterceptor(int streamId, RequestType requestType, long receiveStartTimeNanos) {
            this.streamId = streamId;
            startTime = Clock.now();
            this.requestType = requestType;
            if (eventPublisher.isEventPublishingEnabled()) {
                EventListener eventListener = eventPublisher.getEventListener();
                if (client) {
                    eventListener.requestSendStart(streamId, requestType);
                } else {
                    eventListener.responseSendStart(streamId, requestType, Clock.elapsedSince(receiveStartTimeNanos),
                                                    TimeUnit.NANOSECONDS);
                }
            }
        }

        public void sendComplete() {
            if (eventPublisher.isEventPublishingEnabled()) {
                EventListener eventListener = eventPublisher.getEventListener();
                if (client) {
                    eventListener
                            .requestSendComplete(streamId, requestType, Clock.elapsedSince(startTime), Clock.unit());
                } else {
                    eventListener
                            .responseSendComplete(streamId, requestType, Clock.elapsedSince(startTime), Clock.unit());
                }
            }
        }

        public void sendFailed(Throwable cause) {
            if (eventPublisher.isEventPublishingEnabled()) {
                EventListener eventListener = eventPublisher.getEventListener();
                if (client) {
                    eventListener.requestSendFailed(streamId, requestType, Clock.elapsedSince(startTime),
                                                    Clock.unit(), cause);
                } else {
                    eventListener.responseSendFailed(streamId, requestType, Clock.elapsedSince(startTime), Clock.unit(),
                                                     cause);
                }
            }
        }

        public void sendCancelled() {
            if (eventPublisher.isEventPublishingEnabled()) {
                EventListener eventListener = eventPublisher.getEventListener();
                if (client) {
                    eventListener.requestSendCancelled(streamId, requestType, Clock.elapsedSince(startTime),
                                                       Clock.unit());
                } else {
                    eventListener.responseSendCancelled(streamId, requestType, Clock.elapsedSince(startTime),
                                                        Clock.unit());
                }
            }
        }
    }
}
