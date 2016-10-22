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

import io.reactivex.Flowable;
import io.reactivex.subscribers.TestSubscriber;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

public class PxTest {

    @Test(timeout = 2000)
    public void testMap() throws Exception {
        TestSubscriber<String> testSubscriber = TestSubscriber.create();
        Px.from(Flowable.range(1, 5))
          .map(String::valueOf)
          .subscribe(testSubscriber);

        final List<String> expected = Stream.of(1, 2, 3, 4, 5).map(String::valueOf).collect(Collectors.toList());
        testSubscriber.await().assertComplete().assertNoErrors().assertValueCount(5).assertValueSequence(expected);
    }

    @Test(timeout = 2000)
    public void testJustWithDelayedDemand() throws Exception {
        TestSubscriber<String> subscriber = TestSubscriber.create(0);
        Px.just("Hello").subscribe(subscriber);
        subscriber.assertValueCount(0)
                  .assertNoErrors()
                  .assertNotComplete();

        subscriber.request(1);

        subscriber.assertValueCount(1)
                  .assertValues("Hello")
                  .assertNoErrors()
                  .assertComplete();
    }

    @Test(timeout = 2000)
    public void testDefer() throws Exception {
        final AtomicInteger factoryInvoked = new AtomicInteger();
        TestSubscriber<String> subscriber = TestSubscriber.create();
        Px<String> defer = Px.defer(() -> {
            factoryInvoked.incrementAndGet();
            return Px.just("Hello");
        });

        assertThat("Publisher created before subscription.", factoryInvoked.get(), is(0));
        defer.subscribe(subscriber);
        assertThat("Publisher not created on subscription.", factoryInvoked.get(), is(1));
        subscriber.assertComplete().assertNoErrors().assertValues("Hello");
    }
}