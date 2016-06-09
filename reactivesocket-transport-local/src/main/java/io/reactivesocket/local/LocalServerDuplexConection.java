/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.reactivesocket.local;

import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.rx.Completable;
import io.reactivesocket.rx.Observable;
import io.reactivesocket.rx.Observer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;

class LocalServerDuplexConection implements DuplexConnection {
    private final String name;

    private final CopyOnWriteArrayList<Observer<Frame>> subjects;

    public LocalServerDuplexConection(String name) {
        this.name = name;
        this.subjects = new CopyOnWriteArrayList<>();
    }

    @Override
    public Observable<Frame> getInput() {
        return o -> {
            o.onSubscribe(() -> subjects.removeIf(s -> s == o));
            subjects.add(o);
        };
    }

    @Override
    public void addOutput(Publisher<Frame> o, Completable callback) {
        o
            .subscribe(new Subscriber<Frame>() {

                @Override
                public void onSubscribe(Subscription s) {
                    s.request(Long.MAX_VALUE);
                }

                @Override
                public void onNext(Frame frame) {
                    try {
                        LocalReactiveSocketManager
                            .getInstance()
                            .getClientConnection(name)
                            .write(frame);
                    } catch (Throwable t) {
                        onError(t);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    callback.error(t);
                }

                @Override
                public void onComplete() {
                    callback.success();
                }
            });
    }

    @Override
    public double availability() {
        return 1.0;
    }

    void write(Frame frame) {
        subjects
            .forEach(o -> o.onNext(frame));
    }

    @Override
    public void close() throws IOException {
        LocalReactiveSocketManager
            .getInstance()
            .removeServerDuplexConnection(name);

    }
}
