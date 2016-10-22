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

import org.reactivestreams.Publisher;
import io.reactivesocket.reactivestreams.extensions.internal.ValidatingSubscription;

import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class TestScheduler implements Scheduler {

    private long time;

    private final Queue<TimedAction> queue = new PriorityQueue<TimedAction>(11);

    @Override
    public Publisher<Void> timer(long time, TimeUnit unit) {
        return s -> {
            ValidatingSubscription<? super Void> sub = ValidatingSubscription.empty(s);
            queue.add(new TimedAction(this.time + unit.toNanos(time), sub));
            s.onSubscribe(sub);
        };
    }

    /**
     * Moves the Scheduler's clock forward by a specified amount of time.
     *
     * @param delayTime the amount of time to move the Scheduler's clock forward
     * @param unit the units of time that {@code delayTime} is expressed in
     */
    public void advanceTimeBy(long delayTime, TimeUnit unit) {
        triggerActionsForNanos(time + unit.toNanos(delayTime));
    }

    private void triggerActionsForNanos(long targetTimeNanos) {
        while (!queue.isEmpty()) {
            TimedAction current = queue.peek();
            if (current.timeNanos > targetTimeNanos) {
                break;
            }
            time = current.timeNanos;
            queue.remove();

            // Only execute if active
            if (current.subscription.isActive()) {
                current.subscription.safeOnComplete();
            }
        }
        time = targetTimeNanos;
    }

    private static class TimedAction implements Comparable<TimedAction> {

        private static final AtomicLong counter = new AtomicLong();
        private final long timeNanos;
        private final ValidatingSubscription<? super Void> subscription;
        private final long count = counter.incrementAndGet(); // for differentiating tasks at same timeNanos

        private TimedAction(long timeNanos, ValidatingSubscription<? super Void> subscription) {
            this.timeNanos = timeNanos;
            this.subscription = subscription;
        }

        @Override
        public String toString() {
            return String.format("TimedAction(timeNanos = %d)", timeNanos);
        }

        @Override
        public int compareTo(TimedAction o) {
            if (timeNanos == o.timeNanos) {
                return Long.compare(count, o.count);
            }
            return Long.compare(timeNanos, o.timeNanos);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TimedAction)) {
                return false;
            }

            TimedAction that = (TimedAction) o;

            if (timeNanos != that.timeNanos) {
                return false;
            }
            if (count != that.count) {
                return false;
            }
            if (subscription != null? !subscription.equals(that.subscription) : that.subscription != null) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = (int) (timeNanos ^ timeNanos >>> 32);
            result = 31 * result + (subscription != null? subscription.hashCode() : 0);
            result = 31 * result + (int) (count ^ count >>> 32);
            return result;
        }
    }
}
