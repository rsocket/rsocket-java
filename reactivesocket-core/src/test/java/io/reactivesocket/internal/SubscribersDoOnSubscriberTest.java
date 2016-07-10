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

import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.*;

public class SubscribersDoOnSubscriberTest {

    @Rule
    public final SubscriberRule rule = new SubscriberRule();

    @Test
    public void testSubscribe() throws Exception {
        CancellableSubscriber<Void> subscriber = Subscribers.create(rule.getDoOnSubscribe(),
                                                                    rule.getDoOnCancel());
        AtomicInteger subscriptionCancelCount = rule.subscribe(subscriber);
        rule.assertOnSubscribe(1);
        subscriber.cancel();
        rule.assertOnCancel(1);
        MatcherAssert.assertThat("Subscription not cancelled.", subscriptionCancelCount.get(), is(1));
    }

    @Test
    public void testDuplicateSubscribe() throws Exception {
        CancellableSubscriber<String> subscriber = rule.subscribe();
        rule.assertOnSubscribe(1);

        rule.subscribe(subscriber);
        rule.assertOnSubscribe(1);
        rule.assertOnError(1);
    }

    @Test
    public void testDuplicateCancel() throws Exception {
        CancellableSubscriber<Void> subscriber = Subscribers.create(rule.getDoOnSubscribe(),
                                                                    rule.getDoOnCancel());
        AtomicInteger subscriptionCancelCount = rule.subscribe(subscriber);
        rule.assertOnSubscribe(1);
        subscriber.cancel();
        rule.assertOnCancel(1);
        MatcherAssert.assertThat("Subscription not cancelled.", subscriptionCancelCount.get(), is(1));

        subscriber.cancel();
        rule.assertOnCancel(1);
        rule.assertOnError(0);
    }

}
