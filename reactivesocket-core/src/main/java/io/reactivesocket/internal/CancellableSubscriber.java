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

package io.reactivesocket.internal;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

abstract class CancellableSubscriber<T> implements Subscriber<T> {

    private Subscription s;
    private boolean cancelled;

    @Override
    public void onSubscribe(Subscription s) {
        boolean _cancel = false;
        synchronized (this) {
            this.s = s;
            if (cancelled) {
                _cancel = true;
            }
        }

        if (_cancel) {
            _unsafeCancel();
        }
    }

    public void cancel() {
        boolean _cancel = false;
        synchronized (this) {
            cancelled = true;
            if (s != null) {
                _cancel = true;
            }
        }

        if (_cancel) {
            _unsafeCancel();
        }
    }

    protected void doAfterCancel() {
        // NoOp by default.
    }

    private void _unsafeCancel() {
        s.cancel();
        doAfterCancel();
    }
}
