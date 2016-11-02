/*
 * Copyright 2016 Netflix, Inc.
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

package io.reactivesocket.perf.util;

import io.reactivesocket.Payload;
import org.openjdk.jmh.infra.Blackhole;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class BlackholeSubscriber<T> implements Subscriber<T> {

    private final Blackhole blackhole;
    private final Runnable onTerminate;

    public BlackholeSubscriber(Blackhole blackhole, Runnable onTerminate) {
        this.blackhole = blackhole;
        this.onTerminate = onTerminate;
    }

    @Override
    public void onSubscribe(Subscription s) {
        s.request(1);
    }

    @Override
    public void onNext(T payload) {
        blackhole.consume(payload);
    }

    @Override
    public void onError(Throwable t) {
        t.printStackTrace();
        onTerminate.run();
    }

    @Override
    public void onComplete() {
        onTerminate.run();
    }
}
