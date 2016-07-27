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

import org.hamcrest.MatcherAssert;
import org.junit.Rule;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

public class SubscribersCreateTest {

    @Rule
    public final SubscriberRule rule = new SubscriberRule();

    @Test(timeout = 10000)
    public void testOnNext() throws Exception {
        CancellableSubscriber<String> subscriber = rule.subscribe();
        subscriber.onNext("Hello");
        rule.assertOnNext(1);
        rule.getTestSubscriber().assertValue("Hello");
    }

    @Test(timeout = 10000)
    public void testOnError() throws Exception {
        CancellableSubscriber<String> subscriber = rule.subscribe();
        subscriber.onNext("Hello");
        rule.assertOnNext(1);
        rule.getTestSubscriber().assertValue("Hello");

        subscriber.onError(new NullPointerException());
        rule.assertOnError(1);
        rule.getTestSubscriber().assertError(NullPointerException.class);
    }

    @Test(timeout = 10000)
    public void testOnComplete() throws Exception {
        CancellableSubscriber<String> subscriber = rule.subscribe();
        subscriber.onNext("Hello");
        rule.assertOnNext(1);
        rule.getTestSubscriber().assertValue("Hello");

        subscriber.onComplete();
        rule.assertOnComplete(1);
        rule.getTestSubscriber().assertComplete();
    }

    @Test(timeout = 10000)
    public void testOnNextAfterComplete() throws Exception {
        CancellableSubscriber<String> subscriber = rule.subscribe();
        rule.assertOnSubscribe(1);
        subscriber.onNext("Hello");
        rule.assertOnNext(1);

        subscriber.onComplete();
        rule.assertOnComplete(1);

        subscriber.onNext("Hello");
        rule.assertOnNext(1);
    }

    @Test(timeout = 10000)
    public void testOnNextAfterError() throws Exception {
        CancellableSubscriber<String> subscriber = rule.subscribe();
        rule.assertOnSubscribe(1);
        subscriber.onNext("Hello");
        rule.assertOnNext(1);

        subscriber.onError(new NullPointerException());
        rule.assertOnError(1);
        rule.getTestSubscriber().assertError(NullPointerException.class);

        subscriber.onNext("Hello");
        rule.assertOnNext(1);
    }
}
