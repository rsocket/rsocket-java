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

package io.rsocket;

import io.rsocket.test.util.TestDuplexConnection;
import io.reactivex.subscribers.TestSubscriber;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.concurrent.ConcurrentLinkedQueue;

public abstract class AbstractSocketRule<T extends ReactiveSocket> extends ExternalResource {

    protected TestDuplexConnection connection;
    protected TestSubscriber<Void> connectSub;
    protected T socket;
    protected ConcurrentLinkedQueue<Throwable> errors;

    @Override
    public Statement apply(final Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                connection = new TestDuplexConnection();
                connectSub = TestSubscriber.create();
                errors = new ConcurrentLinkedQueue<>();
                init();
                base.evaluate();
            }
        };
    }

    protected void init() {
        socket = newReactiveSocket();
    }

    protected abstract T newReactiveSocket();
}
