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
package io.reactivesocket.aeron.client;

import io.aeron.Publication;
import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.aeron.internal.Loggable;
import io.reactivesocket.aeron.internal.NotConnectedException;
import io.reactivesocket.exceptions.TransportException;
import io.reactivesocket.rx.Completable;
import org.agrona.concurrent.AbstractConcurrentArrayQueue;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.DirectProcessor;

import java.io.IOException;
import java.util.function.Consumer;

public class AeronClientDuplexConnection implements DuplexConnection, Loggable {

    private final Publication publication;
    private DirectProcessor<Frame> directProcessor;
    private final AbstractConcurrentArrayQueue<FrameHolder> frameSendQueue;
    private final Consumer<Publication> onClose;

    public AeronClientDuplexConnection(
        Publication publication,
        AbstractConcurrentArrayQueue<FrameHolder> frameSendQueue,
        Consumer<Publication> onClose) {
        this.publication = publication;
        this.directProcessor = DirectProcessor.create();
        this.frameSendQueue = frameSendQueue;
        this.onClose = onClose;
    }

    @Override
    public final Publisher<Frame> getInput() {
        if (isTraceEnabled()) {
            trace("getting input for publication session id {} ", publication.sessionId());
        }

        return directProcessor;
    }

    @Override
    public void addOutput(Publisher<Frame> o, Completable callback) {
        o
            .subscribe(new Subscriber<Frame>() {
                private Subscription subscription;

                @Override
                public void onSubscribe(Subscription s) {
                    this.subscription = s;
                    s.request(128);

                }

                @Override
                public void onNext(Frame frame) {
                    if (isTraceEnabled()) {
                        trace("onNext subscription => {} and frame => {}", subscription.toString(), frame.toString());
                    }

                    final FrameHolder fh = FrameHolder.get(frame, publication, subscription);
                    boolean offer;
                    do {
                        offer = frameSendQueue.offer(fh);
                    } while (!offer);
                }

                @Override
                public void onError(Throwable t) {
                    if (t instanceof NotConnectedException) {
                        callback.error(new TransportException(t));
                        subscription.cancel();
                    } else {
                        callback.error(t);
                    }
                }

                @Override
                public void onComplete() {
                    callback.success();
                }
            });
    }

    @Override
    public double availability() {
        return publication.isClosed() ? 0.0 : 1.0;
    }

    @Override
    public void close() throws IOException {
        onClose.accept(publication);
        directProcessor.onComplete();
    }

    public DirectProcessor<Frame> getProcessor() {
        return directProcessor;
    }

    public String toString() {
        if (publication == null) {
            return  getClass().getName() + ":publication=null";
        }

        return getClass().getName() + ":publication=[" +
            "channel=" + publication.channel() + "," +
            "streamId=" + publication.streamId() + "," +
            "sessionId=" + publication.sessionId() + "]";
    }
}
