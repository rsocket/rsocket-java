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
package io.reactivesocket.client.filter;

import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.ReactiveSocketFactory;
import org.reactivestreams.Publisher;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class TimeoutFactory<T> implements ReactiveSocketFactory<T> {
    private final ReactiveSocketFactory<T> child;
    private final ScheduledExecutorService executor;
    private final long timeout;
    private final TimeUnit unit;

    public TimeoutFactory(ReactiveSocketFactory<T> child, long timeout, TimeUnit unit, ScheduledExecutorService executor) {
        this.child = child;
        this.timeout = timeout;
        this.unit = unit;
        this.executor = executor;
    }

    public TimeoutFactory(ReactiveSocketFactory<T> child, long timeout, TimeUnit unit) {
        this(child, timeout, unit, Executors.newScheduledThreadPool(2));
    }

    @Override
    public Publisher<ReactiveSocket> apply() {
        return subscriber ->
            child.apply().subscribe(new TimeoutSubscriber<>(subscriber, executor, timeout, unit));
    }

    @Override
    public double availability() {
        return child.availability();
    }

    @Override
    public T remote() {
        return child.remote();
    }

    @Override
    public String toString() {
        return "TimeoutFactory(" + timeout + " " + unit.toString() + ")->" + child.toString();
    }

    public static <T> Function<ReactiveSocketFactory<T>, ReactiveSocketFactory<T>> filter(long timeout, TimeUnit unit) {
        return f -> new TimeoutFactory<>(f, timeout, unit);
    }
}
