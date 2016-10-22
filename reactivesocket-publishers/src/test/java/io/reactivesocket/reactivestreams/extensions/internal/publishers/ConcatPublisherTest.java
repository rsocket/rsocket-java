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
import io.reactivex.subscribers.TestSubscriber;

public class ConcatPublisherTest {

    @Test
    public void testBothSuccess() throws Exception {
        TestSubscriber<Integer> subscriber = TestSubscriber.create();
        Px.just(1).concatWith(Px.just(2)).subscribe(subscriber);
        subscriber.assertComplete().assertNoErrors().assertValueCount(2).assertValues(1, 2);
    }

    @Test
    public void testFirstError() throws Exception {
        TestSubscriber<Integer> subscriber = TestSubscriber.create();
        Px.<Integer>error(new NullPointerException("Deliberate exception"))
          .concatWith(Px.just(2)).subscribe(subscriber);
        subscriber.assertNotComplete().assertError(NullPointerException.class)
                  .assertNoValues();
    }

    @Test
    public void testSecondError() throws Exception {
        TestSubscriber<Integer> subscriber = TestSubscriber.create();
        Px.just(1).concatWith(Px.error(new NullPointerException("Deliberate exception")))
          .subscribe(subscriber);
        subscriber.assertNotComplete().assertError(NullPointerException.class)
                  .assertValueCount(1).assertValues(1);
    }

    @Test
    public void testDelayedDemand() throws Exception {
        TestSubscriber<Integer> subscriber = TestSubscriber.create(0);
        Px.just(1).concatWith(Px.just(2))
          .subscribe(subscriber);

        subscriber.assertNotTerminated();
        subscriber.request(1);
        subscriber.assertNotTerminated().assertValueCount(1).assertValues(1);

        subscriber.request(1);
        subscriber.assertComplete().assertNoErrors().assertValueCount(2).assertValues(1, 2);
    }

    @Test
    public void testOverlappingDemand() throws Exception {
        TestSubscriber<Integer> subscriber = TestSubscriber.create(0);
        Flowable.just(1, 2, 3, 4).concatWith(Flowable.just(5, 6, 7, 8))
                .subscribe(subscriber);

        subscriber.assertNotTerminated();
        subscriber.request(3);
        subscriber.assertNotTerminated().assertValueCount(3).assertValues(1, 2, 3);

        subscriber.request(5);
        subscriber.assertComplete().assertNoErrors().assertValueCount(8).assertValues(1, 2, 3, 4, 5, 6, 7, 8);
    }
}