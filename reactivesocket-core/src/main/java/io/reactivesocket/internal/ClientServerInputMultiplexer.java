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

package io.reactivesocket.internal;

import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import org.agrona.BitUtil;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * {@link DuplexConnection#receive()} is a single stream on which the following type of frames arrive:
 * <ul>
 <li>Frames for streams initiated by the initiator of the connection (client).</li>
 <li>Frames for streams initiated by the acceptor of the connection (server).</li>
 </ul>
 *
 * The only way to differentiate these two frames is determining whether the stream Id is odd or even. Even IDs are
 * for the streams initiated by server and odds are for streams initiated by the client. <p>
 */
public class ClientServerInputMultiplexer {

    private final SourceInput sourceInput;

    public ClientServerInputMultiplexer(DuplexConnection source) {
        sourceInput = new SourceInput(source);
    }

    /**
     * Returns the frames for streams that were initiated by the server.
     *
     * @return The frames for streams that were initiated by the server.
     */
    public Flux<Frame> getServerInput() {
        return sourceInput.evenStream();
    }

    /**
     * Returns the frames for streams that were initiated by the client.
     *
     * @return The frames for streams that were initiated by the client.
     */
    public Flux<Frame> getClientInput() {
        return sourceInput.oddStream();
    }

    public DuplexConnection asServerConnection() {
        return new InternalDuplexConnection(getServerInput());
    }

    public DuplexConnection asClientConnection() {
        return new InternalDuplexConnection(getClientInput());
    }

    private static final class SourceInput implements Subscriber<Frame> {

        private final DuplexConnection source;
        private int subscriberCount; // Guarded by this
        private volatile Subscription sourceSubscription;
        private volatile ValidatingSubscription<? super Frame> oddSubscription;
        private volatile ValidatingSubscription<? super Frame> evenSubscription;

        public SourceInput(DuplexConnection source) {
            this.source = source;
        }

        @Override
        public void onSubscribe(Subscription s) {
            boolean cancelThis;
            synchronized (this) {
                cancelThis = sourceSubscription != null/*ReactiveStreams rule 2.5*/;
                sourceSubscription = s;
            }
            if (cancelThis) {
                s.cancel();
            } else {
                // Start downstream subscriptions only when this subscriber is active. This elimiates any buffering.
                oddSubscription.getSubscriber().onSubscribe(oddSubscription);
                evenSubscription.getSubscriber().onSubscribe(evenSubscription);
            }
        }

        @Override
        public void onNext(Frame frame) {
            if (frame.getStreamId() == 0) {
                evenSubscription.safeOnNext(frame);
                oddSubscription.safeOnNext(frame);
            } else if (BitUtil.isEven(frame.getStreamId())) {
                evenSubscription.safeOnNext(frame);
            } else {
                oddSubscription.safeOnNext(frame);
            }
        }

        @Override
        public void onError(Throwable t) {
            oddSubscription.safeOnError(t);
            evenSubscription.safeOnError(t);
        }

        @Override
        public void onComplete() {
            oddSubscription.safeOnComplete();
            evenSubscription.safeOnComplete();
        }

        public Flux<Frame> oddStream() {
            return Flux.from(s ->
                subscribe(s, true)
            );
        }

        public Flux<Frame> evenStream() {
            return Flux.from(s ->
                subscribe(s, false)
            );
        }

        private void subscribe(Subscriber<? super Frame> s, boolean odd) {
            Throwable sendError = null;
            boolean subscribeUp = false;
            synchronized (this) {
                if(subscriberCount == 0 || subscriberCount == 1) {
                    if (odd) {
                        if (oddSubscription == null) {
                            oddSubscription = newSubscription(s);
                        } else {
                            sendError = new IllegalStateException("An active subscription already exists.");
                        }
                    } else if (evenSubscription == null) {
                        evenSubscription = newSubscription(s);
                    } else {
                        sendError = new IllegalStateException("An active subscription already exists.");
                    }
                    subscriberCount++;
                    subscribeUp = subscriberCount == 2;
                } else {
                    sendError = new IllegalStateException("More than " + 2 + " subscribers received.");
                }
            }

            if (sendError != null) {
                s.onError(sendError);
            } else if(subscribeUp) {
                source.receive().subscribe(this);
            }
        }

        private ValidatingSubscription<? super Frame> newSubscription(Subscriber<? super Frame> s) {
            return ValidatingSubscription.create(s, () -> {
                final boolean cancelUp;
                synchronized (this) {
                    cancelUp = --subscriberCount == 0;
                }
                if (cancelUp) {
                    sourceSubscription.cancel();
                }
            }, requestN -> {
                // Since these are requests from odd/even streams they are for different frames and hence can pass
                // through to upstream.
                sourceSubscription.request(requestN);
            });
        }
    }

    private class InternalDuplexConnection implements DuplexConnection {

        private final Flux<Frame> input;

        InternalDuplexConnection(Flux<Frame> input) {
            this.input = input;
        }

        @Override
        public Mono<Void> send(Publisher<Frame> frame) {
            return sourceInput.source.send(frame);
        }

        @Override
        public Mono<Void> sendOne(Frame frame) {
            return sourceInput.source.sendOne(frame);
        }

        @Override
        public Flux<Frame> receive() {
            return input;
        }

        @Override
        public double availability() {
            return sourceInput.source.availability();
        }

        @Override
        public Mono<Void> close() {
            return sourceInput.source.close();
        }

        @Override
        public Mono<Void> onClose() {
            return sourceInput.source.onClose();
        }
    }
}
