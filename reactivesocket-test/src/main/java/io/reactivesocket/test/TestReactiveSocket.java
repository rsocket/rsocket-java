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
import io.reactivex.Flowable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

public class TestReactiveSocket extends AbstractReactiveSocket {

    @Override
    public Publisher<Payload> requestResponse(Payload payload) {
        return Flowable.just(TestUtil.utf8EncodedPayload("hello world", "metadata"));
    }

    @Override
    public Publisher<Payload> requestStream(Payload payload) {
        return Flowable.fromPublisher(requestResponse(payload)).repeat(10);
    }

    @Override
    public Publisher<Void> fireAndForget(Payload payload) {
        return Subscriber::onComplete;
    }
}
