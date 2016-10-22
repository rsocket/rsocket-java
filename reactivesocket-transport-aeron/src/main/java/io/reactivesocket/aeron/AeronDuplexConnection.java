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
package io.reactivesocket.aeron;

import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.aeron.internal.reactivestreams.AeronChannel;
import io.reactivesocket.aeron.internal.reactivestreams.ReactiveStreamsRemote;
import io.reactivesocket.reactivestreams.extensions.Px;
import io.reactivesocket.reactivestreams.extensions.internal.EmptySubject;
import org.agrona.LangUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.reactivestreams.Publisher;

/**
 * Implementation of {@link DuplexConnection} over Aeron using an {@link io.reactivesocket.aeron.internal.reactivestreams.AeronChannel}
 */
public class AeronDuplexConnection implements DuplexConnection {
    private final String name;
    private final AeronChannel channel;
    private final EmptySubject emptySubject;

    public AeronDuplexConnection(String name, AeronChannel channel) {
        this.name = name;
        this.channel = channel;
        this.emptySubject = new EmptySubject();
    }

    @Override
    public Publisher<Void> send(Publisher<Frame> frame) {
        Px<UnsafeBuffer> buffers = Px.from(frame)
            .map(f -> new UnsafeBuffer(f.getByteBuffer()));

        return channel.send(ReactiveStreamsRemote.In.from(buffers));
    }

    @Override
    public Publisher<Frame> receive() {
        return channel
            .receive()
            .map(b -> Frame.from(b, 0, b.capacity()))
            .doOnError(throwable -> throwable.printStackTrace());
    }

    @Override
    public double availability() {
        return channel.isActive() ? 1.0 : 0.0;
    }

    @Override
    public Publisher<Void> close() {
        return subscriber -> {
            try {
                channel.close();
                emptySubject.onComplete();
            } catch (Exception e) {
                emptySubject.onError(e);
                LangUtil.rethrowUnchecked(e);
            } finally {
                emptySubject.subscribe(subscriber);
            }
        };
    }

    @Override
    public Publisher<Void> onClose() {
        return emptySubject;
    }

    @Override
    public String toString() {
        return "AeronDuplexConnection{" +
            "name='" + name + '\'' +
            ", channel=" + channel +
            ", emptySubject=" + emptySubject +
            '}';
    }
}
