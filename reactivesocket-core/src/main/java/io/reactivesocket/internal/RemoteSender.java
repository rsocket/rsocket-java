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
import io.reactivesocket.Frame.RequestN;
import io.reactivesocket.FrameType;
import io.reactivesocket.reactivestreams.extensions.internal.FlowControlHelper;
import io.reactivesocket.reactivestreams.extensions.internal.ValidatingSubscription;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * An abstraction to convert a {@link Publisher} to send it over a {@code ReactiveSocket} connection. In order to
 * achieve that, this class provides the following contracts:
 *
 * <ul>
 * <li>A {@link Publisher} that can be written to a {@code DuplexConnection} using
 {@link DuplexConnection#send(Publisher)}</li>
 * <li>A {@link Subscriber} that subscribes to the original data {@code Publisher}</li>
 * <li>A {@link Subscription} that can be used to accept cancellations and flow control, typically from the peer of the
 * {@code ReactiveSocket}.</li>
 * </ul>
 *
 * <h2>Subscriptions</h2>
 *
 * Logically there are two subscriptions to this {@code Processor}. One from {@link DuplexConnection#send(Publisher)}
 * and other <em>logical</em> subscription from the peer of the {@code ReactiveSocket} this stream is sent to.
 * The {@link Subscription} contract on this class is to receive {@code Subscription} signals from the remote peer i.e.
 * typically via a {@code RequestN} and {@code Cancel} frames. However, cancellations may be used to signal a cancel of
 * the write, typically due to clean shutdown of the associated {@code ReactiveSocket}.
 *
 * <h2>Flow Control</h2>
 *
 * This class mediates between the flow control between the transport and {@code ReactiveSocket} peer, so
 * to make sure that a higher demand from transport does not overwrite lower demand from the peer and vice-versa.
 * This feature is different than a regular {@link Processor}.
 *
 * <h2>Flow control with terminal events</h2>
 *
 * Since, reactive-streams terminal events ({@code onComplete} and {@code onError}) are not flow controlled and
 * {@code ReactiveSocket} requires the terminal frames to be sent over the wire, this may bring a situation where
 * transport is not available to write and hence these terminal signals have to be buffered. This is the only place
 * where frames are buffered, otherwise, {@link #onNext(Frame)} here would not buffer or check for flow control.
 */
public final class RemoteSender implements Processor<Frame, Frame>, Subscription {

    private final Publisher<Frame> originalSource;
    private final Runnable cleanup;
    private final int streamId;
    private volatile ValidatingSubscription<? super Frame> transportSubscription;
    private volatile Subscription sourceSubscription;

    private int transportRequested; // Guarded by this
    private int remoteRequested; // Guarded by this
    private int outstanding; // Guarded by this
    private Frame bufferedTerminalFrame; // Guarded by this
    private Throwable bufferedTransportError; // Guarded by this

    public RemoteSender(Publisher<Frame> originalSource, Runnable cleanup, int streamId, int initialRemoteRequested) {
        this.originalSource = originalSource;
        this.cleanup = cleanup;
        this.streamId = streamId;
        remoteRequested = initialRemoteRequested;
    }

    public RemoteSender(Publisher<Frame> originalSource, Runnable cleanup, int streamId) {
        this(originalSource, cleanup, streamId, 0);
    }

    @Override
    public void subscribe(Subscriber<? super Frame> s) {
        // Subscription from DuplexConnection (on send)
        ValidatingSubscription<? super Frame> sub;
        synchronized (this) {
            if (transportSubscription != null && transportSubscription.isActive()) {
                throw new IllegalStateException("Duplicate subscriptions not allowed.");
            }
            transportSubscription = ValidatingSubscription.create(s, () -> {
                final Subscription sourceSub;
                synchronized (this) {
                    if (sourceSubscription == null) {
                        return;
                    }
                    sourceSub = sourceSubscription;
                }
                sourceSub.cancel();
                cleanup.run();
            }, requestN -> {
                final Frame bufferedTerminalFrame;
                synchronized (this) {
                    bufferedTerminalFrame = this.bufferedTerminalFrame;
                    transportRequested = FlowControlHelper.incrementRequestN(transportRequested, requestN);
                }
                if (bufferedTerminalFrame != null) {
                    unsafeSendTerminalFrameToTransport(bufferedTerminalFrame, bufferedTransportError);
                    cleanup.run();
                } else {
                    tryRequestN();
                }
            });
            sub = transportSubscription;
        }
        // Starting transport subscription (via onSubscribe) before subscribing to original, so no buffering required.
        s.onSubscribe(sub);
        originalSource.subscribe(this);
    }

    @Override
    public void onSubscribe(Subscription s) {
        boolean cancelThis;
        synchronized (this) {
            cancelThis = sourceSubscription != null/*ReactiveStreams rule 2.5*/ || !transportSubscription.isActive();
            if (!cancelThis) {
                sourceSubscription = s;
            }
        }
        if (cancelThis) {
            s.cancel();
        } else {
            tryRequestN();
        }
    }

    @Override
    public void onNext(Frame frame) {
        // No flow-control check
        FrameType frameType = frame.getType();
        assert frameType != FrameType.ERROR && !isCompleteFrame(frameType);
        synchronized (this) {
            outstanding--;
        }
        transportSubscription.safeOnNext(frame);
    }

    @Override
    public void onError(Throwable t) {
        if (trySendTerminalFrame(Frame.Error.from(streamId, t), t)) {
            transportSubscription.safeOnError(t);
            cleanup.run();
        }
    }

    @Override
    public void onComplete() {
        if (trySendTerminalFrame(Frame.Response.from(streamId, FrameType.COMPLETE), null)) {
            transportSubscription.safeOnComplete();
            cleanup.run();
        }
    }

    public void acceptRequestNFrame(Frame requestNFrame) {
        request(RequestN.requestN(requestNFrame));
    }

    public void acceptCancelFrame(Frame cancelFrame) {
        assert cancelFrame.getType() == FrameType.CANCEL;
        cancel();
    }

    @Override
    public synchronized void request(long requestN) {
        synchronized (this) {
            remoteRequested = FlowControlHelper.incrementRequestN(remoteRequested, requestN);
        }
        tryRequestN();
    }

    @Override
    public void cancel() {
        sourceSubscription.cancel();
        transportSubscription.cancel();
        cleanup.run();
    }

    private void tryRequestN() {
        int _toRequest;
        synchronized (this) {
            if (sourceSubscription == null) {
                return;
            }
            // Request upto remoteRequested but never more than transportRequested.
            _toRequest = Math.min(transportRequested, remoteRequested);
            outstanding = FlowControlHelper.incrementRequestN(outstanding, _toRequest);
            if (outstanding < transportRequested) {
                // Terminal frames are not accounted in remoteRequested, so increment by 1 if transport can accomodate.
                ++outstanding;
            }
            transportRequested -= _toRequest;
            remoteRequested -= _toRequest;
        }

        if (_toRequest > 0) {
            sourceSubscription.request(_toRequest);
        }
    }

    private boolean trySendTerminalFrame(Frame frame, Throwable optionalError) {
        boolean send;
        synchronized (this) {
            send = outstanding > 0;
            if (!send && bufferedTerminalFrame == null) {
                bufferedTerminalFrame = frame;
                bufferedTransportError = optionalError;
            }
        }

        if (send) {
            unsafeSendTerminalFrameToTransport(frame, optionalError);
        }
        return send;
    }

    private void unsafeSendTerminalFrameToTransport(Frame terminalFrame, Throwable optionalError) {
        transportSubscription.safeOnNext(terminalFrame);
        if (terminalFrame.getType() == FrameType.COMPLETE || terminalFrame.getType() == FrameType.NEXT_COMPLETE) {
            transportSubscription.safeOnComplete();
        } else {
            transportSubscription.safeOnError(optionalError);
        }
    }

    private static boolean isCompleteFrame(FrameType frameType) {
        return frameType == FrameType.COMPLETE || frameType == FrameType.NEXT_COMPLETE;
    }
}
