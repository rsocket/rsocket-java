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

package io.reactivesocket.reactivestreams.extensions.internal;

import org.reactivestreams.Subscription;

/**
 * An implementation of {@link Subscription} that allows concatenating multiple subscriptions and takes care of
 * cancelling the previous {@code Subscription} when next {@code Subscription} is provided and passed the remaining
 * requests to the next {@code Subscription}. <p>
 *     It is mandated to use {@link #onItemReceived()} on every item received by the associated {@code Subscriber} to
 *     correctly manage flow control.
 */
public class SerializedSubscription implements Subscription {

    private int requested;
    private Subscription current;
    private boolean cancelled;

    public SerializedSubscription(Subscription first) {
        current = first;
    }

    @Override
    public void request(long n) {
        Subscription subscription;
        synchronized (this) {
            requested = FlowControlHelper.incrementRequestN(requested, n);
            subscription = current;
        }
        if (subscription != null) {
            subscription.request(n);
        }
    }

    @Override
    public void cancel() {
        Subscription subscription;
        synchronized (this) {
            subscription = current;
            cancelled = true;
        }
        subscription.cancel();
    }

    public void cancelCurrent() {
        Subscription subscription;
        synchronized (this) {
            subscription = current;
        }
        subscription.cancel();
    }

    public synchronized void onItemReceived() {
        requested = Math.max(0, requested - 1);
    }

    public void replaceSubscription(Subscription next) {
        Subscription prev;
        int _pendingRequested;
        boolean _cancelled;
        synchronized (this) {
            _pendingRequested = requested;
            _cancelled = cancelled;
            requested = 0; // Reset for next requestN
            prev = current;
            current = next;
        }
        prev.cancel();
        if (_cancelled) {
            next.cancel();
        } else if (_pendingRequested > 0) {
            next.request(_pendingRequested);
        }
    }
}
