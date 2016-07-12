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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A {@code Publisher<Void>} implementation that can only send a termination signal.
 */
public class EmptySubject implements Publisher<Void> {

    private static final Logger logger = LoggerFactory.getLogger(EmptySubject.class);

    private boolean terminated;
    private Throwable optionalError;
    private final List<Subscriber<? super Void>> earlySubscribers = new ArrayList<>();

    @Override
    public void subscribe(Subscriber<? super Void> subscriber) {
        boolean _completed = false;
        final Throwable _error;
        synchronized (this) {
            if (terminated) {
                _completed = true;
            } else {
                earlySubscribers.add(subscriber);
            }
            _error = optionalError;
        }

        if (_completed) {
            if (_error != null) {
                subscriber.onError(_error);
            } else {
                subscriber.onComplete();
            }
        }
    }

    public void onComplete() {
        sendSignalIfRequired(null);
    }

    public void onError(Throwable throwable) {
        sendSignalIfRequired(throwable);
    }

    private void sendSignalIfRequired(Throwable optionalError) {
        List<Subscriber<? super Void>> subs = Collections.emptyList();
        synchronized (this) {
            if (!terminated) {
                terminated = true;
                subs = new ArrayList<>(earlySubscribers);
                earlySubscribers.clear();
                this.optionalError = optionalError;
            }
        }

        for (Subscriber<? super Void> sub : subs) {
            try {
                if (optionalError != null) {
                    sub.onError(optionalError);
                } else {
                    sub.onComplete();
                }
            } catch (Throwable e) {
                logger.error("Error while sending terminal notification. Ignoring the error.", e);
            }
        }
    }
}
