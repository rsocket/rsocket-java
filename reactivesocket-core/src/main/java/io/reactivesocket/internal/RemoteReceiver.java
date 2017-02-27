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
import io.reactivesocket.Payload;
import io.reactivesocket.exceptions.ApplicationException;
import io.reactivesocket.exceptions.CancelException;
import io.reactivesocket.reactivestreams.extensions.internal.FlowControlHelper;
import io.reactivesocket.reactivestreams.extensions.internal.ValidatingSubscription;
import io.reactivesocket.reactivestreams.extensions.internal.subscribers.Subscribers;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.FluxProcessor;

/**
 * An abstraction to receive data from a {@link Publisher} that is available remotely over a {@code ReactiveSocket}
 * connection. In order to achieve that, this class provides the following contracts:
 *
 * <ul>
 * <li>A {@link Publisher} that can be subscribed to receive data from the remote peer.</li>
 * <li>A {@link Subscriber} that subscribes to the original data {@code Publisher} that receives {@code ReactiveSocket}
 * frames from {@link DuplexConnection}</li>
 * </ul>
 *
 * <h2>Flow Control</h2>
 *
 * This class sends {@link Subscription} events over the wire to the remote peer. This is done via
 * {@link DuplexConnection#send(Publisher)} where the stream is the stream of {@code RequestN} and {@code Cancel}
 * frames. <p>
 * {@code RequestN} from the {@code Subscriber} to this {@code Publisher} are sent as-is to the remote peer and the
 * original {@code Publisher} for reading frames. <p>
 * This class honors any write flow control imposed by {@link DuplexConnection#send(Publisher)} to control the
 * {@code RequestN} and {@code Cancel} frames sent over the wire. This means that if the underlying connection isn't
 * ready to write, no frames will be enqueued into the connection. All {@code RequestN} frames sent during such time
 * will be merged into a single {@code RequestN} frame.
 */
public final class RemoteReceiver extends FluxProcessor<Frame, Payload> {

    private final Publisher<Frame> transportSource;
    private final DuplexConnection connection;
    private final int streamId;
    private final Runnable cleanup;
    private final Frame requestFrame;
    private final Subscription transportSubscription;
    private final boolean sendRequestN;
    private volatile ValidatingSubscription<? super Frame> subscription;
    private volatile Subscription sourceSubscription;
    private volatile boolean missedComplete;
    private volatile Throwable missedError;

    public RemoteReceiver(Publisher<Frame> transportSource, DuplexConnection connection, int streamId,
                          Runnable cleanup, boolean sendRequestN) {
        this.transportSource = transportSource;
        this.connection = connection;
        this.streamId = streamId;
        this.cleanup = cleanup;
        this.sendRequestN = sendRequestN;
        requestFrame = null;
        transportSubscription = null;
    }

    public RemoteReceiver(DuplexConnection connection, int streamId, Runnable cleanup, Frame requestFrame,
                          Subscription transportSubscription, boolean sendRequestN) {
        this.requestFrame = requestFrame;
        this.transportSubscription = transportSubscription;
        transportSource = null;
        this.connection = connection;
        this.streamId = streamId;
        this.cleanup = cleanup;
        this.sendRequestN = sendRequestN;
    }

    @Override
    public void subscribe(Subscriber<? super Payload> s) {
        final SubscriptionFramesSource framesSource = new SubscriptionFramesSource();
        boolean _missed;
        synchronized (this) {
            if (subscription != null && subscription.isActive()) {
                throw new IllegalStateException("Duplicate subscriptions not allowed.");
            }
            _missed = missedComplete || null != missedError;
            if (!_missed) {
                // Since, the subscriber to this subscription is not started (via onSubscribe) till we receive
                // onSubscribe on this class, the callbacks here will always find sourceSubscription.
                subscription = ValidatingSubscription.create(s, () -> {
                    sourceSubscription.cancel();
                    framesSource.sendCancel();
                    cleanup.run();
                }, requestN -> {
                    sourceSubscription.request(requestN);
                    if (sendRequestN) {
                        framesSource.sendRequestN(requestN);
                    }
                });
            }
        }

        if (_missed) {
            s.onSubscribe(ValidatingSubscription.empty());
            if (null != missedError) {
                s.onError(missedError);
            } else {
                s.onComplete();
            }
            return;
        }

        if (transportSource != null) {
            transportSource.subscribe(this);
        } else if (transportSubscription != null) {
            onSubscribe(transportSubscription);
            onNext(requestFrame);
        }
        connection.send(framesSource)
                  .subscribe(Subscribers.doOnError(throwable -> subscription.safeOnError(throwable)));
    }

    @Override
    public void onSubscribe(Subscription s) {
        boolean cancelThis;
        synchronized (this) {
            cancelThis = sourceSubscription != null/*ReactiveStreams rule 2.5*/ || !subscription.isActive();
            if (!cancelThis) {
                sourceSubscription = s;
            }
        }

        if (cancelThis) {
            s.cancel();
        } else {
            // Do not start the subscriber to the Publisher till this Subscriber is started. This avoids race conditions
            // and hence caching of requestN and cancel from downstream without upstream being ready.
            subscription.getSubscriber().onSubscribe(subscription);
        }
    }

    @Override
    public void onNext(Frame frame) {
        synchronized (this) {
            if (subscription == null) {
                throw new IllegalStateException("Received onNext before subscription.");
            }
        }
        switch (frame.getType()) {
        case ERROR:
            onError(new ApplicationException(frame));
            break;
        case NEXT:
            subscription.safeOnNext(frame);
            break;
        case COMPLETE:
            onComplete();
            break;
        case NEXT_COMPLETE:
            subscription.safeOnNext(frame);
            onComplete();
            break;
        }
    }

    @Override
    public void onError(Throwable t) {
        boolean _missed = false;
        synchronized (this) {
            if (subscription == null) {
                _missed = true;
                missedError = t;
            }
        }
        if (!_missed) {
            subscription.safeOnError(t);
        }
        cleanup.run();
    }

    @Override
    public void onComplete() {
        boolean _missed = false;
        synchronized (this) {
            if (subscription == null) {
                _missed = true;
                missedComplete = true;
            }
        }
        if (!_missed) {
            subscription.safeOnComplete();
        }
        cleanup.run();
    }

    public void cancel() {
        sourceSubscription.cancel();
        // Since, source subscription is cancelled, send an error to the subscriber to cleanup.
        onError(new CancelException("Remote subscription cancelled."));
    }

    private class SubscriptionFramesSource implements Publisher<Frame> {

        private ValidatingSubscription<? super Frame> subscription;
        private int requested; // Guarded by this.
        private int bufferedRequestN; // Guarded by this.
        private boolean bufferedCancel; // Guarded by this.

        @Override
        public void subscribe(Subscriber<? super Frame> s) {
            subscription = ValidatingSubscription.onRequestN(s, requestN -> {
                boolean sendCancel;
                int n;
                synchronized (this) {
                    requested = FlowControlHelper.incrementRequestN(requested, requestN);
                    sendCancel = bufferedCancel;
                    n = bufferedRequestN;
                }
                if (sendCancel) {
                    subscription.safeOnNext(Frame.Cancel.from(streamId));
                } else if (sendRequestN && n > 0) {
                    subscription.safeOnNext(Frame.RequestN.from(streamId, n));
                }
            });
            s.onSubscribe(subscription);
        }

        public void sendRequestN(final long n) {
            final int toRequest;
            final ValidatingSubscription<? super Frame> sub;
            synchronized (this) {
                sub = subscription;
                if (requested > 0) {
                    toRequest = FlowControlHelper.incrementRequestN(bufferedRequestN, n);
                    bufferedRequestN = 0; // Reset the buffer since this requestN will be sent.
                    requested--;
                } else {
                    bufferedRequestN = FlowControlHelper.incrementRequestN(bufferedRequestN, n);
                    toRequest = 0;
                }
            }
            if (sub != null && sub.isActive() && toRequest > 0) {
                sub.safeOnNext(Frame.RequestN.from(streamId, toRequest));
            }
        }

        public void sendCancel() {
            final boolean send;
            synchronized (this) {
                send = requested > 0;
                if (send) {
                    requested--;
                } else {
                    bufferedCancel = true;
                }
            }
            if (send) {
                subscription.safeOnNext(Frame.Cancel.from(streamId));
            }
        }
    }
}
