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

import io.reactivex.Observable;
import io.reactivex.subscribers.TestSubscriber;
import org.hamcrest.MatcherAssert;
import org.junit.Test;
import org.reactivestreams.Publisher;

import java.util.concurrent.atomic.AtomicBoolean;

public class PublishersConcatEmptyTest {

    @Test(timeout = 10000)
    public void concatEmpty() throws Exception {
        Publisher<Void> first = Publishers.empty();
        Publisher<Void> second = Publishers.empty();

        Publisher<Void> concat = Publishers.concatEmpty(first, second);
        TestSubscriber<Void> testSubscriber = new TestSubscriber<>();
        concat.subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertNoErrors();
    }

    @Test(timeout = 10000)
    public void concatEmptyFirstError() throws Exception {
        NullPointerException npe = new NullPointerException();
        Publisher<Void> first = Publishers.error(npe);
        AtomicBoolean secondSubscribed = new AtomicBoolean();
        Observable<Void> second = Observable.<Void>empty().doOnSubscribe(subscription -> secondSubscribed.set(true));

        Publisher<Void> concat = Publishers.concatEmpty(first, second);
        TestSubscriber<Void> testSubscriber = new TestSubscriber<>();
        concat.subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertError(npe);
        MatcherAssert.assertThat("Second source was subscribed even though first errored.", !secondSubscribed.get());
    }

    @Test(timeout = 10000)
    public void concatEmptySecondError() throws Exception {
        NullPointerException npe = new NullPointerException();
        AtomicBoolean firstSubscribed = new AtomicBoolean();
        Observable<Void> first = Observable.<Void>empty().doOnSubscribe(subscription -> firstSubscribed.set(true));
        AtomicBoolean secondSubscribed = new AtomicBoolean();
        Observable<Void> second = Observable.<Void>error(npe).doOnSubscribe(subscription -> secondSubscribed.set(true));

        Publisher<Void> concat = Publishers.concatEmpty(first, second);
        TestSubscriber<Void> testSubscriber = new TestSubscriber<>();
        concat.subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertError(npe);
        MatcherAssert.assertThat("First source was not subscribed.", firstSubscribed.get());
        MatcherAssert.assertThat("Second source was not subscribed.", secondSubscribed.get());
    }

    @Test(timeout = 10000)
    public void concatEmptyVerifySubscribe() throws Exception {
        AtomicBoolean firstSubscribed = new AtomicBoolean();
        Observable<Void> first = Observable.<Void>empty().doOnSubscribe(subscription -> firstSubscribed.set(true));
        AtomicBoolean secondSubscribed = new AtomicBoolean();
        Observable<Void> second = Observable.<Void>empty().doOnSubscribe(subscription -> secondSubscribed.set(true));

        Publisher<Void> concat = Publishers.concatEmpty(first, second);
        TestSubscriber<Void> testSubscriber = new TestSubscriber<>();
        concat.subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertNoErrors();

        MatcherAssert.assertThat("First source was not subscribed.", firstSubscribed.get());
        MatcherAssert.assertThat("Second source was not subscribed.", secondSubscribed.get());
    }
}