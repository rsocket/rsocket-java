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
import io.reactivesocket.internal.Publishers;
import io.reactivesocket.util.ReactiveSocketFactoryProxy;
import org.reactivestreams.Publisher;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TimeoutFactory<T> extends ReactiveSocketFactoryProxy<T> {
    private final Publisher<Void> timer;
    private final long timeout;
    private final TimeUnit unit;

    public TimeoutFactory(ReactiveSocketFactory<T> child, long timeout, TimeUnit unit,
                          ScheduledExecutorService executor) {
        super(child);
        this.timeout = timeout;
        this.unit = unit;
        timer = Publishers.timer(executor, timeout, unit);
    }

    @Override
    public Publisher<ReactiveSocket> apply() {
        return Publishers.timeout(super.apply(), timer);
    }

    @Override
    public String toString() {
        return "TimeoutFactory(" + timeout + " " + unit + ")->" + child;
    }
}
