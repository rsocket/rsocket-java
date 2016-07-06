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
import io.reactivex.schedulers.Schedulers;
import io.reactivex.schedulers.TestScheduler;
import io.reactivex.subscribers.TestSubscriber;
import org.junit.Test;
import org.reactivestreams.Publisher;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PublishersMapTest {

    @Test(timeout = 10000)
    public void mapSameType() throws Exception {
        testMap(num -> "Convert: " + num, "Hello1", "Hello2");
    }

    @Test(timeout = 10000)
    public void mapConvertType() throws Exception {
        testMap(num -> "Convert: " + num, 1, 2);
    }

    @Test(timeout = 10000)
    public void mapWithError() throws Exception {
        NullPointerException npe = new NullPointerException();
        Publisher<String> map = Publishers.map(Publishers.<String>error(npe), o -> o);
        TestSubscriber<String> testSubscriber = new TestSubscriber<>();
        map.subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertError(npe);
        testSubscriber.assertNoValues();
    }

    @Test(timeout = 10000)
    public void mapEmpty() throws Exception {
        Publisher<String> map = Publishers.map(Publishers.<String>empty(), o -> o);
        TestSubscriber<String> testSubscriber = new TestSubscriber<>();
        map.subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertNoErrors();
        testSubscriber.assertNoValues();
    }

    @Test(timeout = 10000)
    public void mapWithCancel() throws Exception {
        String msg1 = "Hello1";
        String msg2 = "Hello2";
        String prefix = "Converted: ";
        TestScheduler testScheduler = Schedulers.test();
        Publisher<String> source = Observable.fromArray(msg1, msg2)
                                             .concatWith(Observable.timer(1, TimeUnit.DAYS, testScheduler)
                                                                   .map(String::valueOf));

        Publisher<String> map = Publishers.map(source, s -> prefix + s);

        TestSubscriber<String> testSubscriber = new TestSubscriber<>();
        map.subscribe(testSubscriber);

        testSubscriber.assertNoErrors();

        testSubscriber.assertValueCount(2);

        testSubscriber.assertValues(prefix + msg1, prefix + msg2);
        testSubscriber.assertNotComplete();

        testSubscriber.cancel();

        testScheduler.advanceTimeBy(1, TimeUnit.DAYS);

        testSubscriber.assertNotComplete();
        testSubscriber.assertValueCount(2);
    }

    @SafeVarargs
    private static <T, R> void testMap(Function<T, R> mapFunc, T... msgs) {
        Publisher<T> source = Observable.fromArray(msgs);
        Publisher<R> map = Publishers.map(source, mapFunc);

        TestSubscriber<R> testSubscriber = new TestSubscriber<>();
        map.subscribe(testSubscriber);

        testSubscriber.awaitTerminalEvent();
        testSubscriber.assertNoErrors();

        testSubscriber.assertValueCount(msgs.length);

        testSubscriber.assertValueSequence(Arrays.asList(msgs).stream().map(mapFunc).collect(Collectors.toList()));
    }
}