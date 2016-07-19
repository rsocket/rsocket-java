/*
 * Copyright 2016 Facebook, Inc.
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

package io.reactivesocket.tckdrivers.common;

import io.reactivesocket.Payload;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class EchoSubscription implements Subscription {

    private Queue<Tuple<String, String>> q;
    private long numSent = 0;
    private long numRequested = 0;
    private Subscriber<? super Payload> sub;
    private boolean cancelled = false;

    public EchoSubscription(Subscriber<? super Payload> sub) {
        q = new ConcurrentLinkedQueue<>();
        this.sub = sub;
    }

    public void add(Tuple<String, String> payload) {
        q.add(payload);
        if (numSent < numRequested) request(0);
    }

    @Override
    public synchronized void request(long n) {
        numRequested += n;
        while (numSent < numRequested && !q.isEmpty() && !cancelled) {
            Tuple<String, String> tup = q.poll();
            sub.onNext(new PayloadImpl(tup.getK(), tup.getV()));
            numSent++;
        }
    }

    @Override
    public void cancel() {
        cancelled = true;
    }
}
