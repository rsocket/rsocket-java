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

import org.hamcrest.MatcherAssert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import io.reactivesocket.reactivestreams.extensions.Px;
import io.reactivex.subscribers.TestSubscriber;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.*;

@RunWith(Parameterized.class)
public class SingleEmissionPublishersTest {

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {Px.empty(), null, null},
                {Px.error(new NullPointerException()), null, NullPointerException.class},
                {Px.just("Hello"), "Hello", null}
        });
    }

    @Parameter
    public Px<String> source;

    @Parameter(1)
    public String item;

    @Parameter(2)
    public Class<Throwable> error;

    @Test
    public void testNegativeRequestN() throws Exception {
        final AtomicReference<Throwable> error = new AtomicReference<>();
        source.subscribe(new Subscriber<String>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(-1);
            }

            @Override
            public void onNext(String s) {
                // No Op
            }

            @Override
            public void onError(Throwable t) {
                if(error.get() == null) {
                    error.set(t);
                }
            }

            @Override
            public void onComplete() {
                // No Op
            }
        });

        MatcherAssert.assertThat("Unexpected error.", error.get(), is(instanceOf(IllegalArgumentException.class)));
    }

    @Test(timeout = 2000)
    public void testHappyCase() throws Exception {
        TestSubscriber<String> subscriber = TestSubscriber.create();
        source.subscribe(subscriber);

        subscriber.await();

        if (error == null) {
            subscriber.assertNoErrors();
            if (item != null) {
                subscriber.assertValueCount(1).assertValues(item);
            } else {
                subscriber.assertValueCount(0);
            }
        } else {
            subscriber.assertError(error);
        }
    }
}
