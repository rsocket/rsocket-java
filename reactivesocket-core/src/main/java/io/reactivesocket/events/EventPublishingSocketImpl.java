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
import io.reactivesocket.util.Clock;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.TimeUnit;

public class EventPublishingSocketImpl implements EventPublishingSocket {

    private final EventPublisher<? extends EventListener> eventPublisher;
    private final boolean client;

    public EventPublishingSocketImpl(EventPublisher<? extends EventListener> eventPublisher, boolean client) {
        this.eventPublisher = eventPublisher;
        this.client = client;
    }

    @Override
    public <T> Mono<T> decorateReceive(int streamId, Mono<T> stream, RequestType requestType) {
        return Mono.using(
            () -> new ReceiveInterceptor(streamId, requestType, Clock.now()),
            receiveInterceptor -> stream
                .doOnSuccess(t -> receiveInterceptor.receiveComplete())
                .doOnError(receiveInterceptor::receiveFailed)
                .doOnCancel(receiveInterceptor::receiveCancelled),
            receiveInterceptor -> {}
        );
    }

    @Override
    public <T> Flux<T> decorateReceive(int streamId, Flux<T> stream, RequestType requestType) {
        return Flux.using(
            () -> new ReceiveInterceptor(streamId, requestType, Clock.now()),
            receiveInterceptor -> stream
                .doOnComplete(receiveInterceptor::receiveComplete)
                .doOnError(receiveInterceptor::receiveFailed)
                .doOnCancel(receiveInterceptor::receiveCancelled),
            receiveInterceptor -> {}
        );
    }

    @Override
    public Mono<Void> decorateSend(int streamId, Mono<Void> stream, long receiveStartTimeNanos, RequestType requestType) {
        return Mono.using(
            () -> new SendInterceptor(streamId, requestType, receiveStartTimeNanos),
            sendInterceptor -> stream
                .doOnSuccess(t -> sendInterceptor.sendComplete())
                .doOnError(sendInterceptor::sendFailed)
                .doOnCancel(sendInterceptor::sendCancelled),
            sendInterceptor -> {}
        );
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
