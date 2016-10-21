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

package io.reactivesocket.test.util;

import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.reactivestreams.extensions.Px;
import org.reactivestreams.Publisher;

import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class MockReactiveSocket implements ReactiveSocket {

    private final AtomicInteger fnfCount;
    private final AtomicInteger rrCount;
    private final AtomicInteger rStreamCount;
    private final AtomicInteger rSubCount;
    private final AtomicInteger rChannelCount;
    private final AtomicInteger pushCount;
    private final ReactiveSocket delegate;

    public MockReactiveSocket(ReactiveSocket delegate) {
        this.delegate = delegate;
        fnfCount = new AtomicInteger();
        rrCount = new AtomicInteger();
        rStreamCount = new AtomicInteger();
        rSubCount = new AtomicInteger();
        rChannelCount = new AtomicInteger();
        pushCount = new AtomicInteger();
    }

    @Override
    public final Publisher<Void> fireAndForget(Payload payload) {
        return Px.from(delegate.fireAndForget(payload))
                 .doOnSubscribe(s -> fnfCount.incrementAndGet());
    }

    @Override
    public final Publisher<Payload> requestResponse(Payload payload) {
        return Px.from(delegate.requestResponse(payload))
                 .doOnSubscribe(s -> rrCount.incrementAndGet());
    }

    @Override
    public final Publisher<Payload> requestStream(Payload payload) {
        return Px.from(delegate.requestStream(payload))
                 .doOnSubscribe(s -> rStreamCount.incrementAndGet());
    }

    @Override
    public final Publisher<Payload> requestSubscription(Payload payload) {
        return Px.from(delegate.requestSubscription(payload))
                 .doOnSubscribe(s -> rSubCount.incrementAndGet());
    }

    @Override
    public final Publisher<Payload> requestChannel(Publisher<Payload> payloads) {
        return Px.from(delegate.requestChannel(payloads))
                 .doOnSubscribe(s -> rChannelCount.incrementAndGet());
    }

    @Override
    public final Publisher<Void> metadataPush(Payload payload) {
        return Px.from(delegate.metadataPush(payload))
                 .doOnSubscribe(s -> pushCount.incrementAndGet());
    }

    @Override
    public double availability() {
        return delegate.availability();
    }

    @Override
    public Publisher<Void> close() {
        return delegate.close();
    }

    @Override
    public Publisher<Void> onClose() {
        return delegate.onClose();
    }

    public void assertFireAndForgetCount(int expected) {
        assertCount(expected, "fire-and-forget", fnfCount);
    }

    public void assertRequestResponseCount(int expected) {
        assertCount(expected, "request-response", rrCount);
    }

    public void assertRequestStreamCount(int expected) {
        assertCount(expected, "request-stream", rStreamCount);
    }

    public void assertRequestSubscriptionCount(int expected) {
        assertCount(expected, "request-subscription", rSubCount);
    }

    public void assertRequestChannelCount(int expected) {
        assertCount(expected, "request-channel", rChannelCount);
    }

    public void assertMetadataPushCount(int expected) {
        assertCount(expected, "metadata-push", pushCount);
    }

    private static void assertCount(int expected, String type, AtomicInteger counter) {
        assertThat("Unexpected invocations for " + type + '.', counter.get(), is(expected));
    }
}
