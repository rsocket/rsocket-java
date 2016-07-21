/*
 * Copyright 2016 Facebook, Inc.
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

package io.reactivesocket.tckdrivers.common;

import io.reactivesocket.Payload;
import io.reactivesocket.internal.frame.ByteBufferUtil;
import io.reactivesocket.util.PayloadImpl;
import io.reactivex.Notification;
import io.reactivex.disposables.Disposable;
import io.reactivex.exceptions.CompositeException;
import io.reactivex.internal.functions.Objects;
import io.reactivex.internal.fuseable.QueueSubscription;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.internal.util.BackpressureHelper;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class TestSubscriber<T> implements Subscriber<T>, Subscription, Disposable {

    /**
     * The actual subscriber to forward events to.
     */
    private final Subscriber<? super Payload> actual;
    /**
     * The initial request amount if not null.
     */
    private final Long initialRequest;
    /**
     * The latch that indicates an onError or onCompleted has been called.
     */
    private final CountDownLatch done;
    /**
     * The list of values received.
     */
    private final List<Tuple<String, String>> values;
    /**
     * The list of errors received.
     */
    private final List<Throwable> errors;
    /**
     * The number of completions.
     */
    private long completions;
    /**
     * The last thread seen by the subscriber.
     */
    private Thread lastThread;

    /**
     * Makes sure the incoming Subscriptions get cancelled immediately.
     */
    private volatile boolean cancelled;

    /**
     * Holds the current subscription if any.
     */
    private final AtomicReference<Subscription> subscription = new AtomicReference<Subscription>();

    /**
     * Holds the requested amount until a subscription arrives.
     */
    private final AtomicLong missedRequested = new AtomicLong();

    /**
     * this will be locked everytime we await at most some number of values, the await will always be with a timeout
     * After the timeout, we look at the value inside the countdown latch to make sure we counted down the
     * number of values we expected
     */
    private CountDownLatch numOnNext = new CountDownLatch(Integer.MAX_VALUE);

    /**
     * This latch handles the logic in take.
     */
    private CountDownLatch takeLatch = new CountDownLatch(Integer.MAX_VALUE);

    /**
     * Keeps track if this test subscriber is passing
     */
    private boolean isPassing = true;

    private boolean isComplete = false;

    /**
     * The echo subscription, if exists
     */
    private EchoSubscription echosub;
    private boolean isEcho = false;

    private boolean checkSubscriptionOnce;

    private int initialFusionMode;

    private int establishedFusionMode;

    private QueueSubscription<Payload> qs;

    /**
     * Constructs a non-forwarding TestSubscriber with an initial request value of Long.MAX_VALUE.
     */
    public TestSubscriber() {
        this(EmptySubscriber.INSTANCE, Long.MAX_VALUE);
    }

    /**
     * Constructs a non-forwarding TestSubscriber with the specified initial request value.
     * <p>The TestSubscriber doesn't validate the initialRequest value so one can
     * test sources with invalid values as well.
     *
     * @param initialRequest the initial request value if not null
     */
    public TestSubscriber(Long initialRequest) {
        this(EmptySubscriber.INSTANCE, initialRequest);
    }

    /**
     * Constructs a forwarding TestSubscriber but leaves the requesting to the wrapped subscriber.
     *
     * @param actual the actual Subscriber to forward events to
     */
    public TestSubscriber(Subscriber<? super Payload> actual) {
        this(actual, null);
    }

    /**
     * Constructs a forwarding TestSubscriber with the specified initial request value.
     * <p>The TestSubscriber doesn't validate the initialRequest value so one can
     * test sources with invalid values as well.
     *
     * @param actual         the actual Subscriber to forward events to
     * @param initialRequest the initial request value if not null
     */
    public TestSubscriber(Subscriber<? super Payload> actual, Long initialRequest) {
        this.actual = actual;
        this.initialRequest = initialRequest;
        this.values = new ArrayList<>();
        this.errors = new ArrayList<Throwable>();
        this.done = new CountDownLatch(1);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onSubscribe(Subscription s) {
        lastThread = Thread.currentThread();

        if (s == null) {
            errors.add(new NullPointerException("onSubscribe received a null Subscription"));
            return;
        }
        if (!subscription.compareAndSet(null, s)) {
            s.cancel();
            if (subscription.get() != SubscriptionHelper.CANCELLED) {
                errors.add(new IllegalStateException("onSubscribe received multiple subscriptions: " + s));
            }
            return;
        }

        if (cancelled) {
            s.cancel();
        }

        if (initialFusionMode != 0) {
            if (s instanceof QueueSubscription) {
                qs = (QueueSubscription<Payload>) s;

                int m = qs.requestFusion(initialFusionMode);
                establishedFusionMode = m;

                if (m == QueueSubscription.SYNC) {
                    checkSubscriptionOnce = true;
                    lastThread = Thread.currentThread();
                    try {
                        Payload t;
                        while ((t = qs.poll()) != null) {
                            values.add(new Tuple<>(ByteBufferUtil.toUtf8String(t.getData()),
                                    ByteBufferUtil.toUtf8String(t.getMetadata())));
                        }
                        completions++;
                    } catch (Throwable ex) {
                        errors.add(ex);
                    }
                    return;
                }
            }
        }


        actual.onSubscribe(s);

        if (cancelled) {
            return;
        }

        if (initialRequest != null) {
            s.request(initialRequest);
        }

        long mr = missedRequested.getAndSet(0L);
        if (mr != 0L) {
            s.request(mr);
        }
    }

    @Override
    public void onNext(T t) {
        Payload p = (Payload) t;
        Tuple<String, String> tup = new Tuple<>(ByteBufferUtil.toUtf8String(p.getData()),
                ByteBufferUtil.toUtf8String(p.getMetadata()));
        System.out.println("ON NEXT GOT : "  + tup.getK() + " " + tup.getV());
        if (isEcho) {
            echosub.add(tup);
            return;
        }
        if (!checkSubscriptionOnce) {
            checkSubscriptionOnce = true;
            if (subscription.get() == null) {
                errors.add(new IllegalStateException("onSubscribe not called in proper order"));
            }
        }
        lastThread = Thread.currentThread();

        if (establishedFusionMode == QueueSubscription.ASYNC) {
            while ((p = qs.poll()) != null) {
                values.add(new Tuple<>(ByteBufferUtil.toUtf8String(p.getData()),
                        ByteBufferUtil.toUtf8String(p.getMetadata())));
            }
            return;
        }

        values.add(tup);
        numOnNext.countDown();
        takeLatch.countDown();

        if (t == null) {
            errors.add(new NullPointerException("onNext received a null Subscription"));
        }

        actual.onNext(new PayloadImpl(tup.getK(), tup.getV()));
    }

    @Override
    public void onError(Throwable t) {
        if (!checkSubscriptionOnce) {
            checkSubscriptionOnce = true;
            if (subscription.get() == null) {
                errors.add(new NullPointerException("onSubscribe not called in proper order"));
            }
        }
        try {
            lastThread = Thread.currentThread();
            errors.add(t);

            if (t == null) {
                errors.add(new IllegalStateException("onError received a null Subscription"));
            }

            actual.onError(t);
        } finally {
            done.countDown();
        }
    }

    @Override
    public void onComplete() {
        isComplete = true;
        if (!checkSubscriptionOnce) {
            checkSubscriptionOnce = true;
            if (subscription.get() == null) {
                errors.add(new IllegalStateException("onSubscribe not called in proper order"));
            }
        }
        try {
            lastThread = Thread.currentThread();
            completions++;

            actual.onComplete();
        } finally {
            done.countDown();
        }
    }

    @Override
    public final void request(long n) {
        if (!SubscriptionHelper.validate(n)) {
            return;
        }
        Subscription s = subscription.get();
        if (s != null) {
            s.request(n);
        } else {
            BackpressureHelper.add(missedRequested, n);
            s = subscription.get();
            if (s != null) {
                long mr = missedRequested.getAndSet(0L);
                if (mr != 0L) {
                    s.request(mr);
                }
            }
        }
    }

    public final void setEcho(EchoSubscription echosub) {
        isEcho = true;
        this.echosub = echosub;
    }

    // there might be a race condition with take, so this behavior is defined as: either wait until we have received n
    // values and then cancel, or cancel if we already have n values
    public final void take(long n) {
        if(values.size() >= n) {
            // if we've already received at least n values, then we cancel
            cancel();
            return;
        }
        while(Integer.MAX_VALUE - takeLatch.getCount() < n) {
            try {
                takeLatch.await(100, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                System.out.println("interrupted");
            }
        }
    }

    @Override
    public final void cancel() {
        if (!cancelled) {
            cancelled = true;
            SubscriptionHelper.dispose(subscription);
        }
    }

    /**
     * Returns true if this TestSubscriber has been cancelled.
     *
     * @return true if this TestSubscriber has been cancelled
     */
    public final boolean isCancelled() {
        if (cancelled) {
            pass("cancelled", cancelled);
        } else {
            fail("cancelled");
        }
        return cancelled;
    }

    @Override
    public final void dispose() {
        cancel();
    }

    @Override
    public final boolean isDisposed() {
        return cancelled;
    }

    // state retrieval methods

    /**
     * Returns the last thread which called the onXXX methods of this TestSubscriber.
     *
     * @return the last thread which called the onXXX methods
     */
    public final Thread lastThread() {
        return lastThread;
    }

    /**
     * Returns a shared list of received onNext values.
     *
     * @return a list of received onNext values
     */
    public final List<Tuple<String, String>> values() {
        return values;
    }

    /**
     * Returns a shared list of received onError exceptions.
     *
     * @return a list of received events onError exceptions
     */
    public final List<Throwable> errors() {
        return errors;
    }

    /**
     * Returns the number of times onComplete was called.
     *
     * @return the number of times onComplete was called
     */
    public final long completions() {
        return completions;
    }

    /**
     * Returns true if TestSubscriber received any onError or onComplete events.
     *
     * @return true if TestSubscriber received any onError or onComplete events
     */
    public final boolean isTerminated() {
        return done.getCount() == 0;
    }

    /**
     * Returns the number of onNext values received.
     *
     * @return the number of onNext values received
     */
    public final int valueCount() {
        return values.size();
    }

    /**
     * Returns the number of onError exceptions received.
     *
     * @return the number of onError exceptions received
     */
    public final int errorCount() {
        return errors.size();
    }

    /**
     * Returns true if this TestSubscriber received a subscription.
     *
     * @return true if this TestSubscriber received a subscription
     */
    public final boolean hasSubscription() {
        return subscription.get() != null;
    }

    /**
     * Awaits until this TestSubscriber receives an onError or onComplete events.
     *
     * @return this
     * @throws InterruptedException if the current thread is interrupted while waiting
     * @see #awaitTerminalEvent()
     */
    public final TestSubscriber await() throws InterruptedException {
        if (done.getCount() == 0) {
            return this;
        }

        done.await();
        return this;
    }

    /**
     * Awaits the specified amount of time or until this TestSubscriber
     * receives an onError or onComplete events, whichever happens first.
     *
     * @param time the waiting time
     * @param unit the time unit of the waiting time
     * @return true if the TestSubscriber terminated, false if timeout happened
     * @throws InterruptedException if the current thread is interrupted while waiting
     * @see #awaitTerminalEvent(long, TimeUnit)
     */
    public final boolean await(long time, TimeUnit unit) throws InterruptedException {
        if (done.getCount() == 0) {
            return true;
        }
        return done.await(time, unit);
    }

    public final boolean awaitAtLeast(long n, long time, TimeUnit unit) throws InterruptedException {
        numOnNext.await(time, unit);
        while (values.size() < n) {
            numOnNext.await(time, unit);
        }
        pass("got " + values.size() + " out of " + n + " values expected", true);
        numOnNext = new CountDownLatch(Integer.MAX_VALUE);
        return true;
    }

    public final void awaitNoEvents(long time) throws InterruptedException {
        int numValues = values.size();
        boolean iscanceled = cancelled;
        boolean iscompleted = isComplete;
        Thread.sleep(time);
        if (numValues == values.size() && iscanceled == cancelled && iscompleted == isComplete) {
            pass("no additional events", true);
        } else {
            fail("received additional events");
        }
    }

    // assertion methods

    /**
     * Fail with the given message and add the sequence of errors as suppressed ones.
     * <p>Note this is delibarately the only fail method. Most of the times an assertion
     * would fail but it is possible it was due to an exception somewhere. This construct
     * will capture those potential errors and report it along with the original failure.
     *
     * @param message the message to use
     * @param errors  the sequence of errors to add as suppressed exception
     */
    private void fail(String prefix, String message, Iterable<? extends Throwable> errors) {
        AssertionError ae = new AssertionError(prefix + message);
        CompositeException ce = new CompositeException();
        for (Throwable e : errors) {
            if (e == null) {
                ce.suppress(new NullPointerException("Throwable was null!"));
            } else {
                ce.suppress(e);
            }
        }
        if (!ce.isEmpty()) {
            ae.initCause(ce);
        }
        isPassing = false;
    }

    private void pass(String message, boolean passed) {
        if (passed) System.out.println("PASSED: " + message);
    }

    private void fail(String message) {
        isPassing = false;
        System.out.println("FAILED: " + message);
        isPassing = false;
    }

    /**
     * Assert that this TestSubscriber received exactly one onComplete event.
     *
     * @return this
     */
    public final TestSubscriber assertComplete() {
        String prefix = "";
        boolean passed = true;
        /*
         * This creates a happens-before relation with the possible completion of the TestSubscriber.
         * Don't move it after the instance reads or into fail()!
         */
        if (done.getCount() != 0) {
            prefix = "Subscriber still running! ";
            fail("subscriber still running");
            passed = false;
        }
        long c = completions;
        if (c == 0) {
            fail(prefix, "Not completed", errors);
            fail("not complete");
            passed = false;
        } else if (c > 1) {
            fail(prefix, "Multiple completions: " + c, errors);
            fail("multiple completes");
            passed = false;
        }
        pass("assert Complete", passed);
        return this;
    }

    /**
     * Assert that this TestSubscriber has not received any onComplete event.
     *
     * @return this
     */
    public final TestSubscriber assertNotComplete() {
        String prefix = "";
        boolean passed = true;
        if (done.getCount() != 0) {
            prefix = "Subscriber still running! ";
        }
        long c = completions;
        if (c == 1) {
            fail(prefix, "Completed!", errors);
            fail("completed");
            passed = false;
        } else if (c > 1) {
            fail(prefix, "Multiple completions: " + c, errors);
            fail("multiple completions");
            passed = false;
        }
        pass("not complete", passed);
        return this;
    }

    /**
     * Assert that this TestSubscriber has not received any onError event.
     *
     * @return this
     */
    public final TestSubscriber assertNoErrors() {
        boolean passed = true;
        String prefix = "";
        if (done.getCount() != 0) {
            prefix = "Subscriber still running! ";
        }
        int s = errors.size();
        if (s != 0) {
            fail(prefix, "Error(s) present: " + errors, errors);
            fail("errors exist");
        }
        pass("no errors", passed);
        return this;
    }

    /**
     * Assert that this TestSubscriber received exactly the specified onError event value.
     * <p>
     * <p>The comparison is performed via Objects.equals(); since most exceptions don't
     * implement equals(), this assertion may fail. Use the {@link #assertError(Class)}
     * overload to test against the class of an error instead of an instance of an error.
     *
     * @param error the error to check
     * @return this
     * @see #assertError(Class)
     */
    public final TestSubscriber assertError(Throwable error) {
        String prefix = "";
        boolean passed = true;
        if (done.getCount() != 0) {
            prefix = "Subscriber still running! ";
        }
        int s = errors.size();
        if (s == 0) {
            fail(prefix, "No errors", Collections.<Throwable>emptyList());
            passed = false;
        }
        /*if (errors.contains(error)) {
            if (s != 1) {
                fail(prefix, "Error present but other errors as well", errors);
                passed = false;
            }
        } else {
            fail(prefix, "Error not present", errors);
            passed = false;
        }*/
        pass("error received", passed);
        return this;
    }

    /**
     * Asserts that this TestSubscriber received exactly one onError event which is an
     * instance of the specified errorClass class.
     *
     * @param errorClass the error class to expect
     * @return this
     */
    public final TestSubscriber assertError(Class<? extends Throwable> errorClass) {
        String prefix = "";
        if (done.getCount() != 0) {
            prefix = "Subscriber still running! ";
        }
        int s = errors.size();
        if (s == 0) {
            fail(prefix, "No errors", Collections.<Throwable>emptyList());
        }

        boolean found = false;

        for (Throwable e : errors) {
            if (errorClass.isInstance(e)) {
                found = true;
                break;
            }
        }

        if (found) {
            if (s != 1) {
                fail(prefix, "Error present but other errors as well", errors);
            }
        } else {
            fail(prefix, "Error not present", errors);
        }
        return this;
    }

    /**
     * Assert that this TestSubscriber received exactly one onNext value which is equal to
     * the given value with respect to Objects.equals.
     *
     * @return this
     */
    public final TestSubscriber assertValue(Tuple<String, String> value) {
        String prefix = "";
        boolean passed = true;
        if (done.getCount() != 0) {
            prefix = "Subscriber still running! ";
        }
        int s = values.size();
        if (s != 1) {
            fail(prefix, "Expected: " + value + ", Actual: " + values, errors);
            fail("value does not match");
            passed = false;
        }
        Tuple<String, String> v = values.get(0);
        if (!Objects.equals(value, v)) {
            fail(prefix, "Expected: " + valueAndClass(value) + ", Actual: " + valueAndClass(v), errors);
            fail("value does not match");
            passed = false;
        }
        pass("value matches", passed);
        return this;
    }

    /**
     * Appends the class name to a non-null value.
     */
    static String valueAndClass(Object o) {
        if (o != null) {
            return o + " (class: " + o.getClass().getSimpleName() + ")";
        }
        return "null";
    }

    /**
     * Assert that this TestSubscriber received the specified number onNext events.
     *
     * @param count the expected number of onNext events
     * @return this
     */
    public final TestSubscriber assertValueCount(int count) {
        String prefix = "";
        boolean passed = true;
        if (done.getCount() != 0) {
            prefix = "Subscriber still running! ";
        }
        int s = values.size();
        if (s != count) {
            fail(prefix, "Value counts differ; Expected: " + count + ", Actual: " + s, errors);
            passed = false;
        }
        pass("received " + count + " values", passed);
        return this;
    }

    public final TestSubscriber assertReceivedAtLeast(int count) {
        String prefix = "";
        boolean passed = true;
        if (done.getCount() != 0) {
            prefix = "Subscriber still running! ";
        }
        int s = values.size();
        if (s < count) {
            fail(prefix, "Received less; Expected at least: " + count + ", Actual: " + s, errors);
            passed = false;
        }
        pass("received " + s + " values", passed);
        return this;
    }

    /**
     * Assert that this TestSubscriber has not received any onNext events.
     *
     * @return this
     */
    public final TestSubscriber assertNoValues() {
        return assertValueCount(0);
    }

    /**
     * Assert that the TestSubscriber received only the specified values in the specified order.
     *
     * @param values the values expected
     * @return this
     * @see #assertValueSet(Collection)
     */
    public final TestSubscriber assertValues(List<Tuple<String, String>> values) {
        String prefix = "";
        boolean passed = true;
        if (done.getCount() != 0) {
            prefix = "Subscriber still running! ";
        }
        int s = this.values.size();
        if (s != values.size()) {
            fail(prefix, "Value count differs; Expected: " + values.size() + " " + values
                    + ", Actual: " + s + " " + this.values, errors);
            passed = false;
            fail("length incorrect");
        }
        for (int i = 0; i < s; i++) {
            Tuple<String, String> v = this.values.get(i);
            Tuple<String, String> u = values.get(i);
            if (!Objects.equals(u, v)) {
                fail(prefix, "Values at position " + i + " differ; Expected: "
                        + valueAndClass(u) + ", Actual: " + valueAndClass(v), errors);
                passed = false;
                fail("value does not match");
            }
        }
        pass("all values match", passed);
        return this;
    }

    /**
     * Assert that the TestSubscriber received only the specified values in any order.
     * <p>This helps asserting when the order of the values is not guaranteed, i.e., when merging
     * asynchronous streams.
     *
     * @param values the collection of values expected in any order
     * @return this
     */
    public final TestSubscriber assertValueSet(Collection<? extends Tuple<String, String>> values) {
        String prefix = "";
        if (done.getCount() != 0) {
            prefix = "Subscriber still running! ";
        }
        int s = this.values.size();
        if (s != values.size()) {
            fail(prefix, "Value count differs; Expected: " + values.size() + " " + values
                    + ", Actual: " + s + " " + this.values, errors);
        }
        for (int i = 0; i < s; i++) {
            Tuple<String, String> v = this.values.get(i);

            if (!values.contains(v)) {
                fail(prefix, "Value not in the expected collection: " + valueAndClass(v), errors);
            }
        }
        return this;
    }

    /**
     * Assert that the TestSubscriber received only the specified sequence of values in the same order.
     *
     * @param sequence the sequence of expected values in order
     * @return this
     */
    public final TestSubscriber assertValueSequence(Iterable<? extends Tuple<String, String>> sequence) {
        String prefix = "";
        if (done.getCount() != 0) {
            prefix = "Subscriber still running! ";
        }
        int i = 0;
        Iterator<Tuple<String, String>> vit = values.iterator();
        Iterator<? extends Tuple<String, String>> it = sequence.iterator();
        boolean itNext = false;
        boolean vitNext = false;
        while ((itNext = it.hasNext()) && (vitNext = vit.hasNext())) {
            Tuple<String, String> v = it.next();
            Tuple<String, String> u = vit.next();

            if (!Objects.equals(u, v)) {
                fail(prefix, "Values at position " + i + " differ; Expected: "
                        + valueAndClass(u) + ", Actual: " + valueAndClass(v), errors);
            }
            i++;
        }

        if (itNext && !vitNext) {
            fail(prefix, "More values received than expected (" + i + ")", errors);
        }
        if (!itNext && !vitNext) {
            fail(prefix, "Fever values received than expected (" + i + ")", errors);
        }
        return this;
    }

    /**
     * Assert that the TestSubscriber terminated (i.e., the terminal latch reached zero).
     *
     * @return this
     */
    public final TestSubscriber assertTerminated() {
        if (done.getCount() != 0) {
            fail("", "Subscriber still running!", errors);
        }
        long c = completions;
        if (c > 1) {
            fail("", "Terminated with multiple completions: " + c, errors);
        }
        int s = errors.size();
        if (s > 1) {
            fail("", "Terminated with multiple errors: " + s, errors);
        }

        if (c != 0 && s != 0) {
            fail("", "Terminated with multiple completions and errors: " + c, errors);
        }
        return this;
    }

    /**
     * Assert that the TestSubscriber has not terminated (i.e., the terminal latch is still non-zero).
     *
     * @return this
     */
    public final TestSubscriber assertNotTerminated() {
        if (done.getCount() == 0) {
            fail("", "Subscriber terminated!", errors);
        }
        return this;
    }

    /**
     * Assert that the onSubscribe method was called exactly once.
     *
     * @return this
     */
    public final TestSubscriber assertSubscribed() {
        String prefix = "";
        if (done.getCount() != 0) {
            prefix = "Subscriber still running! ";
        }
        if (subscription.get() == null) {
            fail(prefix, "Not subscribed!", errors);
        }
        return this;
    }

    /**
     * Assert that the onSubscribe method hasn't been called at all.
     *
     * @return this
     */
    public final TestSubscriber assertNotSubscribed() {
        String prefix = "";
        if (done.getCount() != 0) {
            prefix = "Subscriber still running! ";
        }
        if (subscription.get() != null) {
            fail(prefix, "Subscribed!", errors);
        } else if (!errors.isEmpty()) {
            fail(prefix, "Not subscribed but errors found", errors);
        }
        return this;
    }

    /**
     * Waits until the any terminal event has been received by this TestSubscriber
     * or returns false if the wait has been interrupted.
     *
     * @return true if the TestSubscriber terminated, false if the wait has been interrupted
     */
    public final boolean awaitTerminalEvent() {
        try {
            await();
            return true;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Awaits the specified amount of time or until this TestSubscriber
     * receives an onError or onComplete events, whichever happens first.
     *
     * @param duration the waiting time
     * @param unit     the time unit of the waiting time
     * @return true if the TestSubscriber terminated, false if timeout or interrupt happened
     */
    public final boolean awaitTerminalEvent(long duration, TimeUnit unit) {
        try {
            return await(duration, unit);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Assert that there is only a single error with the given message.
     *
     * @param message the message to check
     * @return this
     */
    public final TestSubscriber assertErrorMessage(String message) {
        String prefix = "";
        if (done.getCount() != 0) {
            prefix = "Subscriber still running! ";
        }
        int s = errors.size();
        if (s == 0) {
            fail(prefix, "No errors", Collections.<Throwable>emptyList());
        } else if (s == 1) {
            Throwable e = errors.get(0);
            if (e == null) {
                fail(prefix, "Error is null", Collections.<Throwable>emptyList());
            }
            String errorMessage = e.getMessage();
            if (!Objects.equals(message, errorMessage)) {
                fail(prefix, "Error message differs; Expected: " + message + ", Actual: "
                        + errorMessage, Collections.singletonList(e));
            }
        } else {
            fail(prefix, "Multiple errors", errors);
        }
        return this;
    }

    /**
     * Returns a list of 3 other lists: the first inner list contains the plain
     * values received; the second list contains the potential errors
     * and the final list contains the potential completions as Notifications.
     *
     * @return a list of (values, errors, completion-notifications)
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public final List<List<Object>> getEvents() {
        List<List<Object>> result = new ArrayList<List<Object>>();

        result.add((List) values());

        result.add((List) errors());

        List<Object> completeList = new ArrayList<Object>();
        for (long i = 0; i < completions; i++) {
            completeList.add(Notification.complete());
        }
        result.add(completeList);

        return result;
    }

    /**
     * Sets the initial fusion mode if the upstream supports fusion.
     *
     * @param mode the mode to establish, see the {@link QueueSubscription} constants
     * @return this
     */
    public final TestSubscriber setInitialFusionMode(int mode) {
        this.initialFusionMode = mode;
        return this;
    }

    /**
     * Asserts that the given fusion mode has been established
     *
     * @param mode the expected mode
     * @return this
     */
    public final TestSubscriber assertFusionMode(int mode) {
        if (establishedFusionMode != mode) {
            if (qs != null) {
                throw new AssertionError("Fusion mode different. Expected: " + mode + ", actual: " + establishedFusionMode);
            } else {
                throw new AssertionError("Upstream is not fuseable");
            }
        }
        return this;
    }

    /**
     * Assert that the upstream is a fuseable source.
     *
     * @return this
     */
    public TestSubscriber assertFuseable() {
        if (qs == null) {
            throw new AssertionError("Upstream is not fuseable.");
        }
        return this;
    }

    /**
     * Assert that the upstream is not a fuseable source.
     *
     * @return this
     */
    public TestSubscriber assertNotFuseable() {
        if (qs != null) {
            throw new AssertionError("Upstream is fuseable.");
        }
        return this;
    }

    /**
     * A subscriber that ignores all events and does not report errors.
     */
    private enum EmptySubscriber implements Subscriber<Object> {
        INSTANCE;

        @Override
        public void onSubscribe(Subscription s) {
        }

        @Override
        public void onNext(Object t) {
        }

        @Override
        public void onError(Throwable t) {
        }

        @Override
        public void onComplete() {
        }
    }

    /**
     * Returns true if the testsubscriber has passed all the assertions, otherwise false
     * @return true if passed
     */
    public boolean hasPassed() {
        return isPassing;
    }

    /**
     * Gets the nth element this subscriber received
     * @param n the index of the element you want
     * @return the nth element
     */
    public Tuple<String, String> getElement(int n) {
        return this.values.get(n);
    }

}
