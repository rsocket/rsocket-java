/**
 * Copyright 2015 Netflix, Inc.
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
package io.reactivesocket.aeron.server.rx;

import io.reactivesocket.aeron.server.ServerAeronManager;
import rx.Scheduler;
import rx.Subscription;
import rx.functions.Action0;

import java.util.concurrent.TimeUnit;

/**
 * An implementation of {@link Scheduler} that lets you schedule work on the {@link ServerAeronManager} polling thread.
 * The work is scheduled on to the thread use a {@link org.agrona.TimerWheel}. This is useful if you have done work on another
 * thread, and than want the work to end up back on the polling thread.
 */
public class ReactiveSocketAeronScheduler extends Scheduler {
    private static final ReactiveSocketAeronScheduler instance = new ReactiveSocketAeronScheduler();

    private ReactiveSocketAeronScheduler() {}

    public static ReactiveSocketAeronScheduler getInstance() {
        return instance;
    }

    @Override
    public Worker createWorker() {
        return new Worker();
    }

    static class Worker extends Scheduler.Worker {
        private volatile boolean subscribed = true;

        @Override
        public Subscription schedule(Action0 action) {
            boolean submitted;
            do {
                submitted = ServerAeronManager.getInstance().submitAction(action);
            } while (!submitted);

            return this;
        }

        @Override
        public Subscription schedule(Action0 action, long delayTime, TimeUnit unit) {
            boolean scheduled;
            do {
                scheduled = ServerAeronManager.getInstance().threadSafeTimeout(delayTime, unit, action);
            } while (!scheduled);

            return this;
        }

        @Override
        public void unsubscribe() {
            subscribed = false;
        }

        @Override
        public boolean isUnsubscribed() {
            return !subscribed;
        }
    }
}
