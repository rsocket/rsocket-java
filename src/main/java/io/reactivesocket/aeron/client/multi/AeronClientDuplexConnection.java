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
package io.reactivesocket.aeron.client.multi;


import io.reactivesocket.Frame;
import io.reactivesocket.aeron.client.AbstractClientDuplexConnection;
import io.reactivesocket.aeron.internal.Constants;
import io.reactivesocket.rx.Completable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import rx.exceptions.MissingBackpressureException;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.agrona.concurrent.ManyToOneConcurrentArrayQueue;

public class AeronClientDuplexConnection extends AbstractClientDuplexConnection<ManyToOneConcurrentArrayQueue<FrameHolder>, FrameHolder> {
    public AeronClientDuplexConnection(Publication publication) {
        super(publication);
    }

    @Override
    protected ManyToOneConcurrentArrayQueue<FrameHolder> createQueue() {
        return new ManyToOneConcurrentArrayQueue<>(Constants.CONCURRENCY);
    }

    @Override
    public void addOutput(Publisher<Frame> o, Completable callback) {
        o
            .subscribe(new Subscriber<Frame>() {
                private Subscription s;

                @Override
                public void onSubscribe(Subscription s) {
                    this.s = s;
                    s.request(Constants.CONCURRENCY);
                }

                @Override
                public void onNext(Frame frame) {
                    final FrameHolder fh = FrameHolder.get(frame, s);
                    boolean offer;
                    int i = 0;
                    do {
                        offer = framesSendQueue.offer(fh);
                        if (!offer && ++i > Constants.MULTI_THREADED_SPIN_LIMIT) {
                            rx.Observable.error(new MissingBackpressureException());
                        }
                    } while (!offer);
                }

                @Override
                public void onError(Throwable t) {
                    callback.error(t);
                }

                @Override
                public void onComplete() {
                    callback.success();
                }
            });
    }

    @Override
    public void close() {
    }

}
