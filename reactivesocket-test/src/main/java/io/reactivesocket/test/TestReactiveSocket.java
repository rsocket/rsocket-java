/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.reactivesocket.test;

import io.reactivesocket.AbstractReactiveSocket;
import io.reactivesocket.Payload;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class TestReactiveSocket extends AbstractReactiveSocket {

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
        return Mono.just(TestUtil.utf8EncodedPayload("hello world", "metadata"));
    }

    @Override
    public Flux<Payload> requestStream(Payload payload) {
        return Flux
            .range(1, 10_000)
            .flatMap(l -> requestResponse(payload));
    }

    @Override
    public Mono<Void> fireAndForget(Payload payload) {
        return Mono.empty();
    }
}
