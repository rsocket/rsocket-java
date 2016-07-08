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

package io.reactivesocket.internal;

import io.reactivesocket.exceptions.TimeoutException;
import io.reactivex.Observable;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.schedulers.TestScheduler;
import io.reactivex.subscribers.TestSubscriber;
import org.junit.Test;
import org.reactivestreams.Publisher;

import java.util.concurrent.TimeUnit;

public class PublishersTimeoutTest {

    @Test(timeout = 10000)
    public void timeoutNotTriggeredSingleMessage() throws Exception {
        String msg = "Hello";
        Publisher<String> source = Publishers.just(msg);
        TestScheduler testScheduler = Schedulers.test();
        Publisher<Void> timer = Observable.timer(1, TimeUnit.DAYS, testScheduler)
                                          .ignoreElements().cast(Void.class);
        Publisher<String> timeout = Publishers.timeout(source, timer);
        TestSubscriber<String> testSubscriber = new TestSubscriber<>();
        timeout.subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertNoErrors();
        testSubscriber.assertValueCount(1);
        testSubscriber.assertValue(msg);
    }

    @Test(timeout = 10000)
    public void timeoutTriggeredPostFirstMessage() throws Exception {
        String msg = "Hello";
        Publisher<String> source = Observable.just(msg)
                                             .concatWith(Observable.never());
        TestScheduler testScheduler = Schedulers.test();
        Publisher<Void> timer = Observable.timer(1, TimeUnit.DAYS, testScheduler)
                                          .ignoreElements().cast(Void.class);
        Publisher<String> timeout = Publishers.timeout(source, timer);
        TestSubscriber<String> testSubscriber = new TestSubscriber<>();
        timeout.subscribe(testSubscriber);

        testSubscriber.assertNoErrors();
        testSubscriber.assertValueCount(1);
        testSubscriber.assertValue(msg);

        testScheduler.advanceTimeBy(1, TimeUnit.DAYS);
        testSubscriber.assertNotTerminated();
        testSubscriber.assertValueCount(1);
    }

    @Test(timeout = 10000)
    public void timeoutTriggeredBeforeFirstMessage() throws Exception {
        Publisher<String> source = Observable.never();
        TestScheduler testScheduler = Schedulers.test();
        Publisher<Void> timer = Observable.timer(1, TimeUnit.DAYS, testScheduler)
                                          .ignoreElements().cast(Void.class);
        Publisher<String> timeout = Publishers.timeout(source, timer);
        TestSubscriber<String> testSubscriber = new TestSubscriber<>();
        timeout.subscribe(testSubscriber);

        testScheduler.advanceTimeBy(1, TimeUnit.DAYS);
        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertError(TimeoutException.class);
        testSubscriber.assertNoValues();
    }
}