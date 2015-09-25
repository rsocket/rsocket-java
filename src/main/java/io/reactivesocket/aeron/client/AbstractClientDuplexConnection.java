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
package io.reactivesocket.aeron.client;

import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.rx.Disposable;
import io.reactivesocket.rx.Observable;
import io.reactivesocket.rx.Observer;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.agrona.concurrent.AbstractConcurrentArrayQueue;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractClientDuplexConnection<T extends AbstractConcurrentArrayQueue<F>, F> implements DuplexConnection {
    protected final static AtomicInteger count = new AtomicInteger();

    protected final int connectionId;

    protected final CopyOnWriteArrayList<Observer<Frame>> subjects;

    protected final T framesSendQueue;

    public AbstractClientDuplexConnection(Publication publication) {
        this.subjects = new CopyOnWriteArrayList<>();
        this.framesSendQueue = createQueue();
        this.connectionId = count.incrementAndGet();
    }

    @Override
    public final Observable<Frame> getInput() {
        return new Observable<Frame>() {
            public void subscribe(Observer<Frame> o) {
                o.onSubscribe(new Disposable() {
                    @Override
                    public void dispose() {
                        subjects.removeIf(s -> s == o);
                    }
                });
                subjects.add(o);
            }
        };
    }

    public final List<? extends Observer<Frame>> getSubscriber() {
        return subjects;
    }

    public final T getFramesSendQueue() {
        return framesSendQueue;
    }

    protected abstract T createQueue();

    public int getConnectionId() {
        return connectionId;
    }
}
