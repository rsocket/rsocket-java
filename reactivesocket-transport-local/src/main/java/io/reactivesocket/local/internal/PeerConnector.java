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
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.nio.channels.ClosedChannelException;

public class PeerConnector {

    private static final Logger logger = LoggerFactory.getLogger(PeerConnector.class);

    private final LocalDuplexConnection client;
    private final LocalDuplexConnection server;
    private final String name;
    private final MonoProcessor<Void> closeNotifier;

    private PeerConnector(String name) {
        this.name = name;
        closeNotifier = MonoProcessor.create();
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

    private final class LocalDuplexConnection implements DuplexConnection {

        private volatile ValidatingSubscription<Frame> receiver;
        private volatile boolean connected;
        private final MonoProcessor<Void> closeNotifier;
        private final boolean client;
        private volatile LocalDuplexConnection peer;

        private LocalDuplexConnection(MonoProcessor<Void> closeNotifier, boolean client) {
            this.closeNotifier = closeNotifier;
            this.client = client;
            closeNotifier
                .doFinally(signalType -> {
                    connected = false;
                    if (receiver != null) {
                        receiver.safeOnError(new ClosedChannelException());
                    }
                })
                .subscribe();
        }

        @Override
        public Mono<Void> send(Publisher<Frame> frames) {
            return Flux
                .from(frames)
                .doOnNext(frame -> {
                    if (peer != null) {
                        peer.receiveFrameFromPeer(frame);
                    } else {
                        logger.warn("Sending a frame but peer not connected. Ignoring frame: " + frame);
                    }
                })
                .then();
        }

        @Override
        public Flux<Frame> receive() {
            return Flux.from(sub -> {
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
            });
        }

        @Override
        public double availability() {
            return connected ? 1.0 : 0.0;
        }

        @Override
        public Mono<Void> close() {
            return Mono.defer(() -> {
                closeNotifier.onComplete();
                return closeNotifier;
            });
        }

        @Override
        public Mono<Void> onClose() {
            return closeNotifier;
        }

        @Override
        public String toString() {
            return "[local connection(" + (client ? "client" : "server" + ") - ") + name + "] connected: " + connected;
        }

        public void receiveFrameFromPeer(Frame frame) {
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
