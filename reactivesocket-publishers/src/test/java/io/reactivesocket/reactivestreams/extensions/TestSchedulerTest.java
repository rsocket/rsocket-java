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

import io.reactivesocket.reactivestreams.extensions.TestScheduler;
import org.junit.Test;
import org.reactivestreams.Publisher;
import io.reactivex.subscribers.TestSubscriber;

import java.util.concurrent.TimeUnit;

public class TestSchedulerTest {

    @Test
    public void testTimer() throws Exception {
        TestScheduler scheduler = new TestScheduler();
        TestSubscriber<Void> timer1 = newTimer(scheduler, 1, TimeUnit.HOURS);
        TestSubscriber<Void> timer2 = newTimer(scheduler, 2, TimeUnit.HOURS);

        scheduler.advanceTimeBy(1, TimeUnit.HOURS);
        timer1.assertComplete().assertNoErrors().assertNoValues();
        timer2.assertNotTerminated();

        scheduler.advanceTimeBy(1, TimeUnit.HOURS);
        timer2.assertComplete().assertNoErrors().assertNoValues();
    }

    private static TestSubscriber<Void> newTimer(TestScheduler scheduler, int time, TimeUnit unit) {
        Publisher<Void> timer = scheduler.timer(time, unit);
        TestSubscriber<Void> subscriber = TestSubscriber.create();
        timer.subscribe(subscriber);
        return subscriber;
    }
}