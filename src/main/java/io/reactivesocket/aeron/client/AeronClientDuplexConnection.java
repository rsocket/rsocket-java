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
package io.reactivesocket.aeron.client;


import io.reactivesocket.Frame;
import io.reactivesocket.aeron.internal.Constants;
import io.reactivesocket.aeron.internal.Loggable;
import io.reactivesocket.aeron.internal.concurrent.ManyToManyConcurrentArrayQueue;
import io.reactivesocket.rx.Completable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import rx.exceptions.MissingBackpressureException;
import uk.co.real_logic.aeron.Publication;

public class AeronClientDuplexConnection extends AbstractClientDuplexConnection<ManyToManyConcurrentArrayQueue<FrameHolder>, FrameHolder> implements Loggable {
    public AeronClientDuplexConnection(Publication publication) {
        super(publication);
    }

    protected static final ManyToManyConcurrentArrayQueue<FrameHolder> framesSendQueue = new ManyToManyConcurrentArrayQueue<>(65536);

    @Override
    public void addOutput(Publisher<Frame> o, Completable callback) {
        o
            .subscribe(new Subscriber<Frame>() {
                private Subscription s;

                @Override
                public void onSubscribe(Subscription s) {
                    if (isTraceEnabled()) {
                        trace("onSubscribe subscription => {} on connection id {} ", s.toString(), connectionId);
                    }

                    this.s = s;
                    s.request(Constants.CONCURRENCY);

                }

                @Override
                public void onNext(Frame frame) {
                    if (isTraceEnabled()) {
                        trace("onNext subscription => {} on connection id {} frame => {}", s.toString(), connectionId, frame.toString());
                    }

                    final FrameHolder fh = FrameHolder.get(frame, s);
                    boolean offer;
                    do {
                        offer = framesSendQueue.offer(fh);
                        if (!offer) {
                            onError(new MissingBackpressureException());
                        }
                    } while (!offer);
                }

                @Override
                public void onError(Throwable t) {
                    if (isTraceEnabled()) {
                        trace("onError subscription => {} on connection id {} ", s.toString(), connectionId);
                    }

                    callback.error(t);
                }

                @Override
                public void onComplete() {
                    if (isTraceEnabled()) {
                        trace("onComplete subscription => {} on connection id {} ", s.toString(), connectionId);
                    }
                    callback.success();
                }
            });
    }

    @Override
    public void close() {
    }

    @Override
    public String toString() {
        return "AeronClientDuplexConnection => " + connectionId;
    }

    @Override
    public ManyToManyConcurrentArrayQueue<FrameHolder> getFramesSendQueue() {
        return framesSendQueue;
    }
}
