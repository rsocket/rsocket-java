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
import org.reactivestreams.Publisher;
import rx.RxReactiveStreams;
import rx.Subscription;
import rx.subscriptions.BooleanSubscription;

import javax.websocket.Session;
import java.io.IOException;

public class WebSocketDuplexConnection implements DuplexConnection {
    private final Session session;
    private final rx.Observable<Frame> input;

    public WebSocketDuplexConnection(Session session, rx.Observable<Frame> input) {
        this.session = session;
        this.input = input;
    }

    @Override
    public Observable<Frame> getInput() {
        return o -> {
            Subscription subscription = input.subscribe(o::onNext, o::onError, o::onComplete);
            o.onSubscribe(subscription::unsubscribe);
        };
    }

    @Override
    public void addOutput(Publisher<Frame> o, Completable callback) {
        rx.Completable sent = rx.Completable.concat(RxReactiveStreams.toObservable(o).map(frame ->
                rx.Completable.create(s -> {
                    BooleanSubscription bs = new BooleanSubscription();
                    s.onSubscribe(bs);
                    session.getAsyncRemote().sendBinary(frame.getByteBuffer(), result -> {
                        if (!bs.isUnsubscribed()) {
                            if (result.isOK()) {
                                s.onCompleted();
                            } else {
                                s.onError(result.getException());
                            }
                        }
                    });
                })
        ));

        sent.subscribe(new rx.Completable.CompletableSubscriber() {
            @Override
            public void onCompleted() {
                callback.success();
            }

            @Override
            public void onError(Throwable e) {
                callback.error(e);
            }

            @Override
            public void onSubscribe(Subscription s) {
            }
        });
    }

    @Override
    public double availability() {
        return session.isOpen() ? 1.0 : 0.0;
    }

    @Override
    public void close() throws IOException {
        session.close();
    }

    public String toString() {
        if (session == null) {
            return  getClass().getName() + ":session=null";
        }

        return getClass().getName() + ":session=[" + session.toString() + "]";

    }
}
