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
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSource;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSource;

import static rx.RxReactiveStreams.*;

public class TcpDuplexConnection implements DuplexConnection {

    private final Connection<Frame, Frame> connection;
    private final Mono<Void> closeNotifier;
    private final Mono<Void> close;

    public TcpDuplexConnection(Connection<Frame, Frame> connection) {
        this.connection = connection;
        closeNotifier = MonoSource.wrap(toPublisher(connection.closeListener()));
        close = MonoSource.wrap(toPublisher(connection.close()));
    }

    @Override
    public Mono<Void> send(Publisher<Frame> frames) {
        return MonoSource.wrap(toPublisher(connection.writeAndFlushOnEach(toObservable(frames))));
    }

    @Override
    public Flux<Frame> receive() {
        return FluxSource.wrap(toPublisher(connection.getInput()));
    }

    @Override
    public double availability() {
        return connection.unsafeNettyChannel().isActive() ? 1.0 : 0.0;
    }

    @Override
    public Mono<Void> close() {
        return close;
    }

    @Override
    public Mono<Void> onClose() {
        return closeNotifier;
    }

    public String toString() {
        return connection.unsafeNettyChannel().toString();
    }
}
