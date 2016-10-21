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
package io.reactivesocket.aeron.internal.reactivestreams;

import io.aeron.Publication;
import io.aeron.Subscription;
import io.reactivesocket.aeron.internal.EventLoop;
import io.reactivesocket.reactivestreams.extensions.Px;
import org.agrona.DirectBuffer;
import org.reactivestreams.Publisher;

import java.util.Objects;

/**
 *
 */
public class AeronChannel implements ReactiveStreamsRemote.Channel<DirectBuffer>, AutoCloseable {
    private final String name;
    private final Publication destination;
    private final Subscription source;
    private final AeronOutPublisher outPublisher;
    private final EventLoop eventLoop;

    /**
     * Creates on end of a bi-directional channel
     * @param name name of the channel
     * @param destination {@code Publication} to send data to
     * @param source Aeron {@code Subscription} to listen to data on
     * @param eventLoop {@link EventLoop} used to poll data on
     * @param sessionId sessionId between the {@code Publication} and the remote {@code Subscription}
     */
    public AeronChannel(String name, Publication destination, Subscription source, EventLoop eventLoop, int sessionId) {
        this.destination = destination;
        this.source = source;
        this.name = name;
        this.eventLoop = eventLoop;
        this.outPublisher = new AeronOutPublisher(name, sessionId, source, eventLoop);
    }

    /**
     * Subscribes to a stream of DirectBuffers and sends the to an Aeron Publisher
     * @param in
     * @return
     */
    public Publisher<Void> send(ReactiveStreamsRemote.In<? extends DirectBuffer> in) {
        AeronInSubscriber inSubscriber = new AeronInSubscriber(name, destination);
        Objects.requireNonNull(in, "in must not be null");
        return Px.completable(onComplete ->
                in
                    .doOnCompleteOrError(onComplete, t -> { throw new RuntimeException(t); })
                    .subscribe(inSubscriber)
            );
    }

    /**
     * Returns ReactiveStreamsRemote.Out of DirectBuffer that can only be
     * subscribed to once per channel
     *
     * @return ReactiveStreamsRemote.Out of DirectBuffer
     */
    public ReactiveStreamsRemote.Out<? extends DirectBuffer> receive() {
        return outPublisher;
    }

    @Override
    public void close() throws Exception {
        try {
            destination.close();
            source.close();
        } catch (Throwable t) {
            throw new Exception(t);
        }
    }

    @Override
    public String toString() {
        return "AeronChannel{" +
            "name='" + name + '\'' +
            '}';
    }

    @Override
    public boolean isActive() {
        return !destination.isClosed() && !source.isClosed();
    }
}
