/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.reactivesocket.client.filter;

import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.internal.Publishers;
import io.reactivesocket.util.ReactiveSocketProxy;
import org.reactivestreams.Publisher;

import java.util.function.Function;

public class RetrySocket extends ReactiveSocketProxy {
    private final int retry;
    private final Function<Throwable, Boolean> retryThisException;

    public RetrySocket(ReactiveSocket child, int retry, Function<Throwable, Boolean> retryThisException) {
        super(child);
        this.retry = retry;
        this.retryThisException = retryThisException;
    }

    @Override
    public Publisher<Void> fireAndForget(Payload payload) {
        return Publishers.retry(child.fireAndForget(payload), retry, retryThisException);
    }

    @Override
    public Publisher<Payload> requestResponse(Payload payload) {
        return Publishers.retry(child.requestResponse(payload), retry, retryThisException);
    }

    @Override
    public Publisher<Payload> requestStream(Payload payload) {
        return Publishers.retry(child.requestStream(payload), retry, retryThisException);
    }

    @Override
    public Publisher<Payload> requestSubscription(Payload payload) {
        return Publishers.retry(child.requestSubscription(payload), retry, retryThisException);
    }

    @Override
    public Publisher<Payload> requestChannel(Publisher<Payload> payload) {
        return Publishers.retry(child.requestChannel(payload), retry, retryThisException);
    }

    @Override
    public Publisher<Void> metadataPush(Payload payload) {
        return Publishers.retry(child.metadataPush(payload), retry, retryThisException);
    }

    @Override
    public String toString() {
        return "RetrySocket(" + retry + ")->" + child;
    }
}
