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
package io.reactivesocket.javax.websocket;

import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.rx.Completable;
import io.reactivesocket.rx.Observable;
import io.reactivesocket.rx.Observer;
import org.reactivestreams.Publisher;
import rx.RxReactiveStreams;
import rx.Subscriber;

import javax.websocket.*;
import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;

public class WebSocketDuplexConnection implements DuplexConnection {
    private Session session;

    private CopyOnWriteArrayList<Observer<Frame>> observers;

    private WebSocketDuplexConnection(Session session, rx.Observable<Frame> input) {
        this.session = session;
        this.observers = new CopyOnWriteArrayList<>();
        input.subscribe(new Subscriber<Frame>() {
            @Override
            public void onNext(Frame frame) {
                observers.forEach(o -> o.onNext(frame));
            }

            @Override
            public void onError(Throwable e) {
                observers.forEach(o -> o.onError(e));
            }

            @Override
            public void onCompleted() {
                observers.forEach(Observer::onComplete);
            }
        });
    }

    public static WebSocketDuplexConnection create(Session session, rx.Observable<Frame> input) {
        return new WebSocketDuplexConnection(session, input);
    }

    @Override
    public Observable<Frame> getInput() {
        return new Observable<Frame>() {
            @Override
            public void subscribe(Observer<Frame> o) {
                observers.add(o);

                o.onSubscribe(() ->
                    observers.removeIf(s -> s == o)
                );
            }
        };
    }

    @Override
    public void addOutput(Publisher<Frame> o, Completable callback) {
        rx.Observable<Void> sent = RxReactiveStreams.toObservable(o).concatMap(frame ->
            rx.Observable.create(subscriber -> {
                session.getAsyncRemote().sendBinary(frame.getByteBuffer(), result -> {
                    if (result.isOK()) {
                        subscriber.onCompleted();
                    } else {
                        subscriber.onError(result.getException());
                    }
                });
            })
        );

        sent.doOnCompleted(callback::success)
            .doOnError(callback::error)
            .subscribe();
    }

    @Override
    public void close() throws IOException {
        session.close();
    }
}
