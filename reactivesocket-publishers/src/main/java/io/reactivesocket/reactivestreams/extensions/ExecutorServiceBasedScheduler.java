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

import io.reactivesocket.reactivestreams.extensions.internal.ValidatingSubscription;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class ExecutorServiceBasedScheduler implements Scheduler {

    private static final ScheduledExecutorService globalExecutor;

    static {
        globalExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "global-executor-scheduler");
                t.setDaemon(true);
                return t;
            }
        });
    }

    private final ScheduledExecutorService executorService;

    public ExecutorServiceBasedScheduler() {
        this(globalExecutor);
    }

    public ExecutorServiceBasedScheduler(ScheduledExecutorService executorService) {
        this.executorService = executorService;
    }

    @Override
    public Px<Void> timer(long timeout, TimeUnit unit) {
        return subscriber -> {
            AtomicReference<ScheduledFuture<?>> futureRef = new AtomicReference<>();
            final ValidatingSubscription<? super Void> subscription =
                    ValidatingSubscription.onCancel(subscriber, () -> {
                        ScheduledFuture<?> scheduledFuture = futureRef.get();
                        scheduledFuture.cancel(false);
                    });
            futureRef.set(executorService.schedule(() -> {
                subscription.safeOnComplete();
            }, timeout, unit));
            subscriber.onSubscribe(subscription);
        };
    }
}
