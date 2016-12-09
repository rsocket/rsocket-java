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

package io.reactivesocket.events;

import io.reactivesocket.internal.DisabledEventPublisher;
import io.reactivesocket.internal.EventPublisher;
import io.reactivesocket.internal.EventPublisherImpl;

public abstract class AbstractEventSource<T extends EventListener> implements EventSource<T>, EventPublisher<T> {

    private final EventSource<T> delegate;
    private volatile EventPublisher<T> eventPublisher;

    protected AbstractEventSource() {
        eventPublisher = new DisabledEventPublisher<>();
        delegate = new DisabledEventSource<>();
    }

    protected AbstractEventSource(EventSource<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean isEventPublishingEnabled() {
        return eventPublisher.isEventPublishingEnabled();
    }

    @Override
    public EventSubscription subscribe(T listener) {
        EventPublisher<T> oldPublisher = null;
        synchronized (this) {
            if (eventPublisher != null) {
                oldPublisher = eventPublisher;
            }
            eventPublisher = new EventPublisherImpl<>(listener);
        }
        EventSubscription delegateSubscription = delegate.subscribe(listener);
        if (oldPublisher != null) {
            // Dispose old listener and use the new one.
            oldPublisher.cancel();
        }
        return new EventSubscription() {
            @Override
            public void cancel() {
                eventPublisher.cancel();
                delegateSubscription.cancel();
                synchronized (AbstractEventSource.this) {
                    if (eventPublisher == listener) {
                        eventPublisher = null;
                    }
                }
            }
        };
    }

    @Override
    public T getEventListener() {
        return eventPublisher.getEventListener();
    }

    @Override
    public void cancel() {
        eventPublisher.cancel();
    }
}
