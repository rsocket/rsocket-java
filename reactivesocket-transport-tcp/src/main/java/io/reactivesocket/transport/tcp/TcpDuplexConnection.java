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
package io.reactivesocket.transport.tcp;

import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.internal.rx.BooleanDisposable;
import io.reactivesocket.rx.Completable;
import io.reactivesocket.rx.Observable;
import io.reactivesocket.util.ObserverSubscriber;
import io.reactivex.netty.channel.Connection;
import org.reactivestreams.Publisher;
import rx.RxReactiveStreams;
import rx.Subscriber;

import java.io.IOException;

public class TcpDuplexConnection implements DuplexConnection {

    private final Connection<Frame, Frame> connection;
    private final rx.Observable<Frame> input;

    public TcpDuplexConnection(Connection<Frame, Frame> connection) {
        this.connection = connection;
        input = connection.getInput().publish().refCount();
    }

    @Override
    public final Observable<Frame> getInput() {
        return o -> {
            Subscriber<Frame> subscriber = new ObserverSubscriber(o);
            o.onSubscribe(new BooleanDisposable(subscriber::unsubscribe));
            input.unsafeSubscribe(subscriber);
        };
    }

    @Override
    public void addOutput(Publisher<Frame> o, Completable callback) {
        connection.writeAndFlushOnEach(RxReactiveStreams.toObservable(o))
            .subscribe(new Subscriber<Void>() {
                @Override
                public void onCompleted() {
                    callback.success();
                }

                @Override
                public void onError(Throwable e) {
                    callback.error(e);
                }

                @Override
                public void onNext(Void aVoid) {
                    // No Op.
                }
            });
    }

    @Override
    public double availability() {
        return connection.unsafeNettyChannel().isActive() ? 1.0 : 0.0;
    }

    @Override
    public void close() throws IOException {
        connection.closeNow();
    }

    public String toString() {
        return connection.unsafeNettyChannel().toString();
    }
}
