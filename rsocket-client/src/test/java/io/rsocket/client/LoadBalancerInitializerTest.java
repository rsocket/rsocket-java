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

package io.rsocket.client;

import io.rsocket.ReactiveSocket;
import io.reactivex.subscribers.TestSubscriber;
import org.junit.Test;
import reactor.core.publisher.UnicastProcessor;

import java.util.List;

public class LoadBalancerInitializerTest {

    @Test
    public void testEarlySubscribe() throws Exception {
        UnicastProcessor<List<ReactiveSocketClient>> src = UnicastProcessor.create();
        LoadBalancerInitializer initializer = LoadBalancerInitializer.create(src);
        TestSubscriber<ReactiveSocket> subscriber = TestSubscriber.create();
        initializer.connect().subscribe(subscriber);
        subscriber.assertNotTerminated().assertNoValues();

        initializer.run();

        subscriber.assertComplete().assertValueCount(1);
    }

    @Test
    public void testLateSubscribe() throws Exception {
        UnicastProcessor<List<ReactiveSocketClient>> src = UnicastProcessor.create();
        LoadBalancerInitializer initializer = LoadBalancerInitializer.create(src);
        initializer.run();
        TestSubscriber<ReactiveSocket> subscriber = TestSubscriber.create();
        initializer.connect().subscribe(subscriber);
        subscriber.assertComplete().assertValueCount(1);
    }

    @Test
    public void testEarlyAndLateSubscribe() throws Exception {
        UnicastProcessor<List<ReactiveSocketClient>> src = UnicastProcessor.create();
        LoadBalancerInitializer initializer = LoadBalancerInitializer.create(src);
        TestSubscriber<ReactiveSocket> early = TestSubscriber.create();
        initializer.connect().subscribe(early);
        early.assertNotTerminated().assertNoValues();
        initializer.run();
        TestSubscriber<ReactiveSocket> late = TestSubscriber.create();
        initializer.connect().subscribe(late);
        early.assertComplete().assertValueCount(1);
        late.assertComplete().assertValueCount(1);
    }
}