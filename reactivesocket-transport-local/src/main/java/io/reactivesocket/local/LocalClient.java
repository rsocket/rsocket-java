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

package io.reactivesocket.local;

import io.reactivesocket.DuplexConnection;
import io.reactivesocket.local.internal.PeerConnector;
import io.reactivesocket.transport.TransportClient;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link TransportClient} using local transport. This can only connect to a {@link LocalServer} which should be
 * started before creating this client.
 */
public class LocalClient implements TransportClient {

    private final LocalServer peer;
    private final AtomicInteger connIdGenerator;

    private LocalClient(LocalServer peer) {
        this.peer = peer;
        connIdGenerator = new AtomicInteger();
    }

    @Override
    public Publisher<DuplexConnection> connect() {
        return sub -> {
            sub.onSubscribe(new Subscription() {
                private boolean emit = true;

                @Override
                public void request(long n) {
                    synchronized (this) {
                        if (!emit) {
                            return;
                        }
                        emit = false;
                    }

                    if (n < 0) {
                        sub.onError(new IllegalArgumentException("Rule 3.9: n > 0 is required, but it was " + n));
                    } else {
                        PeerConnector peerConnector = PeerConnector.connect(peer.getName(),
                                                                            connIdGenerator.incrementAndGet());
                        try {
                            peer.accept(peerConnector);
                            sub.onNext(peerConnector.forClient());
                            sub.onComplete();
                        } catch (Exception e) {
                            sub.onError(e);
                        }
                    }
                }

                @Override
                public synchronized void cancel() {
                    emit = false;
                }
            });
        };
    }

    /**
     * Creates a new {@code LocalClient} which connects to the passed {@code peer} server.
     *
     * @param peer Peer to connect.
     *
     * @return A new {@code LocalClient}.
     */
    public static LocalClient create(LocalServer peer) {
        return new LocalClient(peer);
    }

    /**
     * Creates a new {@code LocalClient} which connects to a {@link LocalServer} with the passed {@code peerName}. If
     * such a server does not exist, this method will throw an {@link IllegalArgumentException}
     *
     * @param peerName Name of the peer to connect.
     *
     * @return A new {@code LocalClient}.
     *
     * @throws IllegalArgumentException If no server with the passed {@code peerName} is registered.
     */
    public static LocalClient create(String peerName) {
        return create(LocalPeersManager.getServerOrDie(peerName));
    }
}
