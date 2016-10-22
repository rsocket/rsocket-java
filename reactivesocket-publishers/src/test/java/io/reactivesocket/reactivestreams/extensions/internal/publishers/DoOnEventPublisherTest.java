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
import org.reactivestreams.Subscription;
import io.reactivesocket.reactivestreams.extensions.Px;
import io.reactivex.subscribers.TestSubscriber;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

public class DoOnEventPublisherTest {

    @Test(timeout = 2000)
    public void testDoOnSubscribe() throws Exception {
        TestSubscriber<Integer> testSubscriber = TestSubscriber.create();
        final AtomicReference<Subscription> sub = new AtomicReference<>();
        Px.from(Flowable.range(1, 5))
          .doOnSubscribe(subscription -> {
              sub.set(subscription);
          })
          .subscribe(testSubscriber);

        testSubscriber.await().assertComplete().assertNoErrors();
        assertThat("DoOnSubscriber not called.", sub.get(), is(notNullValue()));
    }

    @Test(timeout = 2000)
    public void testDoOnRequest() throws Exception {
        TestSubscriber<Integer> testSubscriber = TestSubscriber.create(1);
        final AtomicLong requested = new AtomicLong();
        Px.from(Flowable.just(1))
          .doOnRequest(requestN -> {
              requested.addAndGet(requestN);
          })
          .subscribe(testSubscriber);

        testSubscriber.await().assertComplete().assertNoErrors().assertValueCount(1);
        assertThat("Unexpected doOnNexts", requested.get(), is(1L));
    }

    @Test(timeout = 2000)
    public void testDoOnCancel() throws Exception {
        TestSubscriber<Void> testSubscriber = TestSubscriber.create();
        final AtomicBoolean cancelled = new AtomicBoolean();
        Px.<Void>never()
                .doOnCancel(() -> {
                    cancelled.set(true);
                })
                .subscribe(testSubscriber);

        testSubscriber.cancel();
        testSubscriber.assertNotTerminated();
        assertThat("DoOnCancel not called.", cancelled.get(), is(true));
    }

    @Test(timeout = 2000)
    public void testDoOnNext() throws Exception {
        TestSubscriber<Integer> testSubscriber = TestSubscriber.create();
        final List<Integer> onNexts = new ArrayList<>();
        Px.from(Flowable.range(1, 5))
          .doOnNext(next -> {
              onNexts.add(next);
          })
          .subscribe(testSubscriber);

        testSubscriber.await().assertComplete().assertNoErrors().assertValueCount(5);
        assertThat("Unexpected doOnNexts", onNexts, contains(1,2,3,4,5));
    }

    @Test(timeout = 2000)
    public void testDoOnError() throws Exception {
        TestSubscriber<Integer> testSubscriber = TestSubscriber.create();
        final AtomicReference<Throwable> err = new AtomicReference<>();
        Px.from(Flowable.<Integer>error(new NullPointerException("Deliberate exception")))
          .doOnError(throwable -> {
              err.set(throwable);
          })
          .subscribe(testSubscriber);

        testSubscriber.await().assertNotComplete().assertError(NullPointerException.class);
        assertThat("Unexpected error received", err.get(), is(instanceOf(NullPointerException.class)));
    }

    @Test(timeout = 2000)
    public void testDoOnComplete() throws Exception {
        TestSubscriber<Void> testSubscriber = TestSubscriber.create();
        final AtomicBoolean completed = new AtomicBoolean();
        Px.<Void>empty()
                .doOnComplete(() -> {
                    completed.set(true);
                })
                .subscribe(testSubscriber);

        testSubscriber.assertComplete().assertNoErrors().assertNoValues();
        assertThat("DoOnComplete not called.", completed.get(), is(true));
    }

    @Test(timeout = 2000)
    public void testDoOnTerminateWithError() throws Exception {
        TestSubscriber<Void> testSubscriber = TestSubscriber.create();
        final AtomicBoolean terminated = new AtomicBoolean();
        Px.<Void>error(new NullPointerException("Deliberate exception"))
                .doOnTerminate(() -> {
                    terminated.set(true);
                })
                .subscribe(testSubscriber);

        testSubscriber.assertNotComplete().assertError(NullPointerException.class);
        assertThat("DoOnTerminate not called.", terminated.get(), is(true));
    }

    @Test(timeout = 2000)
    public void testDoOnTerminateWithCompletion() throws Exception {
        TestSubscriber<Void> testSubscriber = TestSubscriber.create();
        final AtomicBoolean terminated = new AtomicBoolean();
        Px.<Void>empty()
                .doOnTerminate(() -> {
                    terminated.set(true);
                })
                .subscribe(testSubscriber);

        testSubscriber.assertComplete().assertNoErrors();
        assertThat("DoOnTerminate not called.", terminated.get(), is(true));
    }

}