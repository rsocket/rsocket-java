/*
 * Copyright 2016 Netflix, Inc.
 * <p>
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *  <p>
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  <p>
 *  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations under the License.
 */

package io.reactivesocket.events;

import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.events.EventListener.RequestType;
import io.reactivesocket.internal.EventPublisher;
import io.reactivesocket.reactivestreams.extensions.Px;
import io.reactivesocket.reactivestreams.extensions.internal.subscribers.Subscribers;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.LongSupplier;

import static java.util.concurrent.TimeUnit.*;

public final class ConnectionEventInterceptor implements DuplexConnection {

    private static final Logger logger = LoggerFactory.getLogger(ConnectionEventInterceptor.class);

    private final DuplexConnection delegate;
    private final EventPublisher<? extends EventListener> publisher;

    public ConnectionEventInterceptor(DuplexConnection delegate, EventPublisher<? extends EventListener> publisher) {
        this.delegate = delegate;
        this.publisher = publisher;
    }

    @Override
    public Publisher<Void> send(Publisher<Frame> frame) {
        return delegate.send(Px.from(frame)
                               .map(f -> {
                                   try {
                                       publishEventsForFrameWrite(f);
                                   } catch (Exception e) {
                                       logger.info("Error while emitting events for frame " + f
                                                   + " written. Ignoring error.", e);
                                   }
                                   return f;
                               }));
    }

    @Override
    public Publisher<Void> sendOne(Frame frame) {
        return delegate.sendOne(frame);
    }

    @Override
    public Publisher<Frame> receive() {
        return Px.from(delegate.receive())
                 .map(f -> {
                     try {
                         publishEventsForFrameRead(f);
                     } catch (Exception e) {
                         logger.info("Error while emitting events for frame " + f + " read. Ignoring error.", e);
                     }
                     return f;
                 });
    }

    @Override
    public double availability() {
        return delegate.availability();
    }

    @Override
    public Publisher<Void> close() {
        return delegate.close();
    }

    @Override
    public Publisher<Void> onClose() {
        return delegate.onClose();
    }

    private void publishEventsForFrameRead(Frame frameRead) {
        if (!publisher.isEventPublishingEnabled()) {
            return;
        }
        final EventListener listener = publisher.getEventListener();
        listener.frameRead(frameRead.getStreamId(), frameRead.getType());

        switch (frameRead.getType()) {
        case LEASE:
            listener.leaseReceived(Frame.Lease.numberOfRequests(frameRead), Frame.Lease.ttl(frameRead));
            break;
        case ERROR:
            listener.errorReceived(frameRead.getStreamId(), Frame.Error.errorCode(frameRead));
            break;
        }
    }

    private void publishEventsForFrameWrite(Frame frameWritten) {
        if (!publisher.isEventPublishingEnabled()) {
            return;
        }
        final EventListener listener = publisher.getEventListener();
        listener.frameWritten(frameWritten.getStreamId(), frameWritten.getType());

        switch (frameWritten.getType()) {
        case LEASE:
            listener.leaseSent(Frame.Lease.numberOfRequests(frameWritten), Frame.Lease.ttl(frameWritten));
            break;
        case ERROR:
            listener.errorSent(frameWritten.getStreamId(), Frame.Error.errorCode(frameWritten));
            break;
        }
    }
}
