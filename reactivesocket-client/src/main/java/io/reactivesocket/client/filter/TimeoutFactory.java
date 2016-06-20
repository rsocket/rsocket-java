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
import io.reactivesocket.internal.PublisherFunctions;
import io.reactivesocket.util.ReactiveSocketFactoryProxy;
import org.reactivestreams.Publisher;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class TimeoutFactory<T> extends ReactiveSocketFactoryProxy<T> {

    private final Publisher<Void> timer;

    public TimeoutFactory(ReactiveSocketFactory<T> child, long timeout, TimeUnit unit,
                          ScheduledExecutorService executor) {
        super(child);
        timer = PublisherFunctions.timer(executor, timeout, unit);
    }

    @Override
    public Publisher<ReactiveSocket> apply() {
        return PublisherFunctions.timeout(super.apply(), timer);
    }

    public static Function<Publisher<ReactiveSocket>, Publisher<ReactiveSocket>> asChainFunction(long timeout,
                                                                                                 TimeUnit unit,
                                                                                                 ScheduledExecutorService executor) {
        Publisher<Void> timer = PublisherFunctions.timer(executor, timeout, unit);
        return reactiveSocketPublisher -> {
            return PublisherFunctions.timeout(reactiveSocketPublisher, timer);
        };
    }
}
