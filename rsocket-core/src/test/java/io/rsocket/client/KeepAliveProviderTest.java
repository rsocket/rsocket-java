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

package io.rsocket.client;

import io.rsocket.exceptions.ConnectionException;
import io.reactivex.subscribers.TestSubscriber;
import org.junit.Test;
import reactor.core.publisher.Flux;

import java.util.concurrent.atomic.AtomicLong;

public class KeepAliveProviderTest {

    @Test
    public void testEmptyTicks() throws Exception {
        KeepAliveProvider provider = KeepAliveProvider.from(10, 1, Flux.empty(), () -> 1);
        TestSubscriber<Long> subscriber = TestSubscriber.create();
        provider.ticks().subscribe(subscriber);
        subscriber.assertComplete().assertNoErrors().assertNoValues();
    }

    @Test
    public void testTicksWithAck() throws Exception {
        AtomicLong time = new AtomicLong();
        KeepAliveProvider provider = KeepAliveProvider.from(10, 1, Flux.just(1L, 2L), time::longValue);
        TestSubscriber<Long> subscriber = TestSubscriber.create();
        provider.ticks().doOnNext(aLong -> provider.ack()).subscribe(subscriber);
        subscriber.assertNoErrors().assertComplete().assertValues(1L, 2L);
    }

    @Test
    public void testMissingAck() throws Exception {
        AtomicLong time = new AtomicLong();
        KeepAliveProvider provider = KeepAliveProvider.from(10, 1, Flux.just(1L, 2L), () -> time.addAndGet(100));
        TestSubscriber<Long> subscriber = TestSubscriber.create();
        provider.ticks().subscribe(subscriber);
        subscriber.assertError(ConnectionException.class).assertValues(1L);
    }
}