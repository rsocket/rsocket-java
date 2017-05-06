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

package io.rsocket.lease;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.test.util.MockRSocket;
import io.rsocket.util.PayloadImpl;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameter;
import io.reactivex.subscribers.TestSubscriber;
import reactor.core.publisher.Flux;

import java.util.function.LongSupplier;

public abstract class DefaultLeaseTest<T> {

    @Parameter
    public int permits;
    @Parameter(1)
    public int ttl;
    @Parameter(2)
    public Class<? extends Throwable> expectedException;
    @Parameter(3)
    public int expectedInvocations;
    @Parameter(4)
    public LongSupplier currentTimeSupplier;

    @Test
    public void testFireAndForget() throws Exception {
        T state = init();
        TestSubscriber<Void> subscriber = TestSubscriber.create();
        getRSocket(state).fireAndForget(PayloadImpl.EMPTY).subscribe(subscriber);
        subscriber.assertError(expectedException);
        getMockSocket(state).assertFireAndForgetCount(expectedInvocations);
    }

    @Test
    public void testRequestResponse() throws Exception {
        T state = init();
        TestSubscriber<Payload> subscriber = TestSubscriber.create();
        getRSocket(state).requestResponse(PayloadImpl.EMPTY).subscribe(subscriber);
        subscriber.assertError(expectedException);
        getMockSocket(state).assertRequestResponseCount(expectedInvocations);
    }

    @Test
    public void testRequestStream() throws Exception {
        T state = init();
        TestSubscriber<Payload> subscriber = TestSubscriber.create();
        getRSocket(state).requestStream(PayloadImpl.EMPTY).subscribe(subscriber);
        subscriber.assertError(expectedException);
        getMockSocket(state).assertRequestStreamCount(expectedInvocations);
    }

    @Test
    public void testRequestChannel() throws Exception {
        T state = init();
        TestSubscriber<Payload> subscriber = TestSubscriber.create();
        getRSocket(state).requestChannel(Flux.just(PayloadImpl.EMPTY)).subscribe(subscriber);
        subscriber.assertError(expectedException);
        getMockSocket(state).assertRequestChannelCount(expectedInvocations);
    }

    @Test
    public void testMetadataPush() throws Exception {
        T state = init();
        TestSubscriber<Void> subscriber = TestSubscriber.create();
        getRSocket(state).metadataPush(PayloadImpl.EMPTY).subscribe(subscriber);
        subscriber.assertError(expectedException);
        getMockSocket(state).assertMetadataPushCount(expectedInvocations);
    }

    protected abstract T init();

    protected abstract RSocket getRSocket(T state);

    protected abstract MockRSocket getMockSocket(T state);
}
