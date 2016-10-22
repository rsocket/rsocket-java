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

package io.reactivesocket.perfutil;

import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.reactivestreams.extensions.Px;
import io.reactivex.processors.PublishProcessor;
import io.reactivex.subjects.PublishSubject;
import org.reactivestreams.Publisher;

/**
 * An implementation of {@link DuplexConnection} that provides functionality to modify the behavior dynamically.
 */
public class TestDuplexConnection implements DuplexConnection {

    private final PublishProcessor<Frame> send;
    private final PublishProcessor<Frame> receive;

    public TestDuplexConnection(PublishProcessor<Frame> send, PublishProcessor<Frame> receive) {
        this.send = send;
        this.receive = receive;
    }

    @Override
    public Publisher<Void> send(Publisher<Frame> frame) {
        Px
            .from(frame)
            .doOnNext(f -> send.onNext(f))
            .doOnError(t -> {throw new RuntimeException(t); })
            .subscribe();

        return Px.empty();
    }

    @Override
    public Publisher<Frame> receive() {
        return receive;
    }

    @Override
    public double availability() {
        return 1.0;
    }

    @Override
    public Publisher<Void> close() {
        return Px.empty();
    }

    @Override
    public Publisher<Void> onClose() {
        return Px.empty();
    }
}
