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

import io.reactivesocket.reactivestreams.extensions.Px;
import org.junit.Test;
import io.reactivex.subscribers.TestSubscriber;

public class SwitchToPublisherTest {

    @Test
    public void testSwitch() throws Exception {
        TestSubscriber<Integer> subscriber = TestSubscriber.create(0);
        Px.just("Hello")
          .switchTo(item -> Px.just(1).concatWith(Px.just(2)))
          .subscribe(subscriber);

        subscriber.assertNotTerminated().assertNoValues();
        subscriber.request(1);
        subscriber.assertNotTerminated().assertValues(1);
        subscriber.request(1);
        subscriber.assertComplete().assertNoErrors().assertValues(1, 2);
    }

    @Test
    public void testSwitchWithMoreDemand() throws Exception {
        TestSubscriber<Integer> subscriber = TestSubscriber.create(10);
        Px.just("Hello")
          .switchTo(item -> Px.just(1).concatWith(Px.just(2)))
          .subscribe(subscriber);

        subscriber.assertComplete().assertNoErrors().assertValues(1, 2);
    }

    @Test
    public void testSwitchWithEmptyFirst() throws Exception {
        TestSubscriber<Integer> subscriber = TestSubscriber.create(10);
        Px.empty()
          .switchTo(item -> Px.just(1).concatWith(Px.just(2)))
          .subscribe(subscriber);

        subscriber.assertComplete().assertNoErrors().assertNoValues();
    }

    @Test
    public void testSwitchWithErrorFirst() throws Exception {
        TestSubscriber<Integer> subscriber = TestSubscriber.create(10);
        Px.error(new NullPointerException("Deliberate Exception"))
          .switchTo(item -> Px.just(1).concatWith(Px.just(2)))
          .subscribe(subscriber);

        subscriber.assertNotComplete().assertError(NullPointerException.class).assertNoValues();
    }
}