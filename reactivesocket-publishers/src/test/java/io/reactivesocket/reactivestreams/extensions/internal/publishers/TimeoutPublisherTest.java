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

package io.reactivesocket.reactivestreams.extensions.internal.publishers;

import io.reactivex.Flowable;
import org.junit.Test;
import io.reactivesocket.reactivestreams.extensions.Px;
import io.reactivesocket.reactivestreams.extensions.TestScheduler;
import io.reactivex.subscribers.TestSubscriber;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TimeoutPublisherTest {

    @Test
    public void testTimeout() {
        TestScheduler scheduler = new TestScheduler();
        Px<Integer> source = Px.never();
        TimeoutPublisher<Integer> timeoutPublisher = new TimeoutPublisher<>(source, 1, TimeUnit.DAYS, scheduler);
        TestSubscriber<Integer> testSubscriber = TestSubscriber.create();
        timeoutPublisher.subscribe(testSubscriber);

        testSubscriber.assertNotTerminated();

        scheduler.advanceTimeBy(1, TimeUnit.DAYS);

        testSubscriber.assertError(TimeoutException.class);
    }

    @Test
    public void testEmissionBeforeTimeout() {
        Flowable<Integer> source = Flowable.just(1);
        TestScheduler scheduler = new TestScheduler();
        TimeoutPublisher<Integer> timeoutPublisher = new TimeoutPublisher<>(source, 1, TimeUnit.DAYS, scheduler);
        TestSubscriber<Integer> testSubscriber = TestSubscriber.create();
        timeoutPublisher.subscribe(testSubscriber);
        testSubscriber.assertComplete().assertNoErrors().assertValueCount(1);
    }
}