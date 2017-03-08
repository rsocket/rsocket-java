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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface EventPublishingSocket {

    EventPublishingSocket DISABLED = new EventPublishingSocket() {
        @Override
        public <T> Mono<T> decorateReceive(int streamId, Mono<T> stream, RequestType requestType) {
            return stream;
        }

        @Override
        public <T> Flux<T> decorateReceive(int streamId, Flux<T> stream, RequestType requestType) {
            return stream;
        }

        @Override
        public Mono<Void> decorateSend(int streamId, Mono<Void> stream, long receiveStartTimeNanos,
                                             RequestType requestType) {
            return stream;
        }
    };

    <T> Mono<T> decorateReceive(int streamId, Mono<T> stream, RequestType requestType);

    <T> Flux<T> decorateReceive(int streamId, Flux<T> stream, RequestType requestType);

    Mono<Void> decorateSend(int streamId, Mono<Void> stream, long receiveStartTimeNanos,
                             RequestType requestType);

}
