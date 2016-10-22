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
package io.reactivesocket.transport.tcp;

import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivex.netty.channel.Connection;
import org.reactivestreams.Publisher;

import static rx.RxReactiveStreams.*;

public class TcpDuplexConnection implements DuplexConnection {

    private final Connection<Frame, Frame> connection;
    private final Publisher<Void> closeNotifier;
    private final Publisher<Void> close;

    public TcpDuplexConnection(Connection<Frame, Frame> connection) {
        this.connection = connection;
        closeNotifier = toPublisher(connection.closeListener());
        close = toPublisher(connection.close());
    }

    @Override
    public Publisher<Void> send(Publisher<Frame> frames) {
        return toPublisher(connection.writeAndFlushOnEach(toObservable(frames)));
    }

    @Override
    public Publisher<Frame> receive() {
        return toPublisher(connection.getInput());
    }

    @Override
    public double availability() {
        return connection.unsafeNettyChannel().isActive() ? 1.0 : 0.0;
    }

    @Override
    public Publisher<Void> close() {
        return close;
    }

    @Override
    public Publisher<Void> onClose() {
        return closeNotifier;
    }

    public String toString() {
        return connection.unsafeNettyChannel().toString();
    }
}
