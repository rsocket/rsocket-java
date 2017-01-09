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
package io.reactivesocket.spectator.internal;


import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Spectator;
import com.netflix.spectator.api.Tag;

import java.util.List;

/**
 * A {@link Counter} implementation that uses {@link ThreadLocalAdderCounter}
 */
public class ThreadLocalAdderCounter {

    private final ThreadLocalAdder adder = new ThreadLocalAdder();
    private final Counter counter;

    public ThreadLocalAdderCounter(String name, String monitorId) {
        this(Spectator.globalRegistry(), name, monitorId);
    }

    public ThreadLocalAdderCounter(Registry registry, String name, String monitorId) {
        counter = registry.counter(name, "id", monitorId);
    }

    public void increment() {
        adder.increment();
    }

    public void increment(long amount) {
        adder.increment(amount);
    }

    public long get() {
        return adder.get();
    }
}