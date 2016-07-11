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

import io.reactivex.subscribers.TestSubscriber;
import org.hamcrest.MatcherAssert;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.reactivestreams.Subscription;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.is;

public class SubscriberRule extends ExternalResource {

    private Consumer<Subscription> doOnSubscribe;
    private Consumer<Throwable> doOnError;
    private Consumer<String> doOnNext;
    private Runnable doOnComplete;
    private Runnable doOnCancel;
    private TestSubscriber<String> testSubscriber;

    private int doOnCancelCount;
    private int doOnSubscribeCount;
    private int doOnNextCount;
    private int doOnErrorCount;
    private int doOnCompleteCount;

    @Override
    public Statement apply(final Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                testSubscriber = new TestSubscriber<>();
                doOnSubscribe = subscription -> {
                    doOnSubscribeCount++;
                    testSubscriber.onSubscribe(subscription);
                };
                doOnCancel = () -> {
                    doOnCancelCount++;
                };
                doOnNext = str -> {
                    doOnNextCount++;
                    testSubscriber.onNext(str);
                };
                doOnError = throwable -> {
                    doOnErrorCount++;
                    testSubscriber.onError(throwable);
                };
                doOnComplete = () -> {
                    doOnCompleteCount++;
                    testSubscriber.onComplete();
                };
                base.evaluate();
            }
        };
    }

    public CancellableSubscriber<String> subscribe() {
        CancellableSubscriber<String> subscriber =
                Subscribers.create(doOnSubscribe, doOnNext, doOnError, doOnComplete, doOnCancel);
        subscribe(subscriber);
        return subscriber;
    }

    public AtomicInteger subscribe(CancellableSubscriber<?> subscriber) {
        final AtomicInteger subscriptionCancel = new AtomicInteger();
        subscriber.onSubscribe(Subscriptions.forCancel(() -> subscriptionCancel.incrementAndGet()));
        return subscriptionCancel;
    }

    public void assertOnSubscribe(int count) {
        MatcherAssert.assertThat("Unexpected onSubscriber invocation count.", doOnSubscribeCount, is(count));
    }

    public void assertOnCancel(int count) {
        MatcherAssert.assertThat("Unexpected onCancel invocation count.", doOnCancelCount, is(count));
    }

    public void assertOnNext(int count) {
        MatcherAssert.assertThat("Unexpected onNext invocation count.", doOnNextCount, is(count));
    }

    public void assertOnError(int count) {
        MatcherAssert.assertThat("Unexpected onError invocation count.", doOnErrorCount, is(count));
    }

    public void assertOnComplete(int count) {
        MatcherAssert.assertThat("Unexpected onComplete invocation count.", doOnCompleteCount, is(count));
    }

    public TestSubscriber<String> getTestSubscriber() {
        return testSubscriber;
    }

    public Consumer<Subscription> getDoOnSubscribe() {
        return doOnSubscribe;
    }

    public Consumer<Throwable> getDoOnError() {
        return doOnError;
    }

    public Consumer<String> getDoOnNext() {
        return doOnNext;
    }

    public Runnable getDoOnComplete() {
        return doOnComplete;
    }

    public Runnable getDoOnCancel() {
        return doOnCancel;
    }
}
