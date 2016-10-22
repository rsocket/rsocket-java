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

package io.reactivesocket.local.internal;

import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.reactivestreams.extensions.Px;
import io.reactivesocket.reactivestreams.extensions.internal.EmptySubject;
import io.reactivesocket.reactivestreams.extensions.internal.ValidatingSubscription;
import io.reactivesocket.reactivestreams.extensions.internal.subscribers.CancellableSubscriber;
import io.reactivesocket.reactivestreams.extensions.internal.subscribers.Subscribers;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.ClosedChannelException;
import java.util.function.Consumer;

public class PeerConnector {

    private static final Logger logger = LoggerFactory.getLogger(PeerConnector.class);

    private final LocalDuplexConnection client;
    private final LocalDuplexConnection server;
    private final String name;
    private final EmptySubject closeNotifier;

    private PeerConnector(String name) {
        this.name = name;
        closeNotifier = new EmptySubject();
        server = new LocalDuplexConnection(closeNotifier, false);
        client = new LocalDuplexConnection(closeNotifier, true);
        server.connect(client);
        client.connect(server);
    }

    public DuplexConnection forClient() {
        return client;
    }

    public DuplexConnection forServer() {
        return server;
    }

    public void shutdown() {
        closeNotifier.onComplete();
    }

    public static PeerConnector connect(String name, int id) {
        String uniqueName = name + '-' + id;
        return new PeerConnector(uniqueName);
    }

    private final class LocalDuplexConnection implements DuplexConnection, Consumer<Frame> {

        private volatile ValidatingSubscription<Frame> receiver;
        private volatile boolean connected;
        private final EmptySubject closeNotifier;
        private final boolean client;
        private volatile Consumer<Frame> peer;

        private LocalDuplexConnection(EmptySubject closeNotifier, boolean client) {
            this.closeNotifier = closeNotifier;
            this.client = client;
            closeNotifier.subscribe(Subscribers.doOnTerminate(() -> {
                connected = false;
                if (receiver != null) {
                    receiver.safeOnError(new ClosedChannelException());
                }
            }));
        }

        @Override
        public Publisher<Void> send(Publisher<Frame> frames) {
            return s -> {
                CancellableSubscriber<Frame> writeSub = Subscribers.create(subscription -> {
                    subscription.request(Long.MAX_VALUE); // Local transport is not flow controlled.
                }, frame -> {
                    if (peer != null) {
                        peer.accept(frame);
                    } else {
                        logger.warn("Sending a frame but peer not connected. Ignoring frame: " + frame);
                    }
                }, s::onError, s::onComplete, null);
                s.onSubscribe(ValidatingSubscription.onCancel(s, () -> writeSub.cancel()));
                frames.subscribe(writeSub);
            };
        }

        @Override
        public Publisher<Frame> receive() {
            return sub -> {
                boolean invalid = false;
                synchronized (this) {
                    if (receiver != null && receiver.isActive()) {
                        invalid = true;
                    } else {
                        receiver = ValidatingSubscription.empty(sub);
                    }
                }

                if (invalid) {
                    sub.onSubscribe(ValidatingSubscription.empty(sub));
                    sub.onError(new IllegalStateException("Only one active subscription allowed."));
                } else {
                    sub.onSubscribe(receiver);
                }
            };
        }

        @Override
        public double availability() {
            return connected ? 1.0 : 0.0;
        }

        @Override
        public Publisher<Void> close() {
            return Px.defer(() -> {
                closeNotifier.onComplete();
                return closeNotifier;
            });
        }

        @Override
        public Publisher<Void> onClose() {
            return closeNotifier;
        }

        @Override
        public String toString() {
            return "[local connection(" + (client ? "client" : "server" + ") - ") + name + "] connected: " + connected;
        }

        @Override
        public void accept(Frame frame) {
            if (receiver != null) {
                receiver.safeOnNext(frame);
            } else {
                logger.warn("Received a frame but peer not connected. Ignoring frame: " + frame);
            }
        }

        private synchronized void connect(LocalDuplexConnection peer) {
            this.peer = peer;
            connected = true;
        }
    }
}
