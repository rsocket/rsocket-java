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

package io.reactivesocket.reactivestreams.extensions;

import io.reactivesocket.reactivestreams.extensions.Px;
import io.reactivex.subscribers.TestSubscriber;
import org.junit.Test;

public class ConcatTest {

    @Test
    public void testConcatNoError() throws Exception {
        TestSubscriber<Void> subscriber = TestSubscriber.create();
        Px.concatEmpty(Px.empty(), Px.empty()).subscribe(subscriber);
        subscriber.assertComplete().assertNoErrors().assertNoValues();
    }

    @Test
    public void testConcatFirstNeverCompletes() throws Exception {
        TestSubscriber<Void> subscriber = TestSubscriber.create();
        Px.concatEmpty(Px.never(), Px.empty()).subscribe(subscriber);
        subscriber.assertNotComplete().assertNoErrors().assertNoValues();
    }

    @Test
    public void testConcatSecondNeverCompletes() throws Exception {
        TestSubscriber<Void> subscriber = TestSubscriber.create();
        Px.concatEmpty(Px.empty(), Px.never()).subscribe(subscriber);
        subscriber.assertNotComplete().assertNoErrors().assertNoValues();
    }

    @Test
    public void testConcatFirstError() throws Exception {
        TestSubscriber<Void> subscriber = TestSubscriber.create();
        Px.concatEmpty(Px.error(new NullPointerException("Deliberate exception")),
                       Px.never())
          .subscribe(subscriber);

        subscriber.assertNotComplete().assertError(NullPointerException.class).assertNoValues();
    }

    @Test
    public void testConcatSecondError() throws Exception {
        TestSubscriber<Void> subscriber = TestSubscriber.create();
        Px.concatEmpty(Px.empty(),
                       Px.error(new NullPointerException("Deliberate exception")))
          .subscribe(subscriber);

        subscriber.assertNotComplete().assertError(NullPointerException.class).assertNoValues();
    }

    @Test
    public void testConcatBothError() throws Exception {
        TestSubscriber<Void> subscriber = TestSubscriber.create();
        Px.concatEmpty(Px.error(new NullPointerException("Deliberate exception")),
                       Px.error(new IllegalArgumentException("Deliberate exception")))
          .subscribe(subscriber);

        subscriber.assertNotComplete().assertError(NullPointerException.class).assertNoValues();
    }

    @Test
    public void testConcatNoRequested() throws Exception {
        TestSubscriber<Void> subscriber = TestSubscriber.create(0);
        Px.concatEmpty(Px.empty(), Px.empty())
          .subscribe(subscriber);

        subscriber.assertComplete().assertNoErrors().assertNoValues();
    }
}
