/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.reactivesocket.loadbalancer.servo.internal;

import org.agrona.UnsafeAccess;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Fast adder based on http://psy-lob-saw.blogspot.com/2013/06/java-concurrent-counters-by-numbers.html
 */
public class ThreadLocalAdder {
    private final AtomicLong deadThreadSum = new AtomicLong();

    static class PaddedLong1 {
        long p1, p2, p3, p4, p6, p7;
    }

    static class PaddedLong2 extends PaddedLong1 {
        private static final long VALUE_OFFSET;

        static {
            try {
                VALUE_OFFSET = UnsafeAccess.UNSAFE.objectFieldOffset(PaddedLong2.class.getDeclaredField("value"));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        volatile long value;

        public long get() {
            return value;
        }

        public long plainGet() {
            return UnsafeAccess.UNSAFE.getLong(this, VALUE_OFFSET);
        }

        public void lazySet(long v) {
            UnsafeAccess.UNSAFE.putOrderedLong(this, VALUE_OFFSET, v);
        }

    }

    static class PaddedLong3 extends PaddedLong2 {
        long p9, p10, p11, p12, p13, p14;
    }

    final class ThreadAtomicLong extends PaddedLong3 {
        final Thread t = Thread.currentThread();

        public ThreadAtomicLong() {
            counters.add(this);
            counters
                .forEach(tal -> {
                    if (!tal.t.isAlive()) {
                        deadThreadSum.addAndGet(tal.get());
                        counters.remove(tal);
                    }
                });
        }
    }

    private final CopyOnWriteArrayList<ThreadAtomicLong> counters = new CopyOnWriteArrayList<>();

    private final ThreadLocal<ThreadAtomicLong> threadLocalAtomicLong = ThreadLocal.withInitial(ThreadAtomicLong::new);

    public void increment() {
        increment(1);
    }

    public void increment(long amount) {
        ThreadAtomicLong lc = threadLocalAtomicLong.get();
        lc.lazySet(lc.plainGet() + amount);
    }

    public long get() {
        long currentDeadThreadSum;
        long sum;
        do {
            currentDeadThreadSum = deadThreadSum.get();
            sum = 0;
            for (ThreadAtomicLong threadAtomicLong : counters) {
                sum += threadAtomicLong.get();
            }
        } while (currentDeadThreadSum != deadThreadSum.get());
        return sum + currentDeadThreadSum;
    }

}