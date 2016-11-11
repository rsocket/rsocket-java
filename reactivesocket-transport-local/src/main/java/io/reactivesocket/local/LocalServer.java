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
import io.reactivesocket.reactivestreams.extensions.DefaultSubscriber;
import io.reactivesocket.reactivestreams.extensions.Px;
import io.reactivesocket.reactivestreams.extensions.internal.subscribers.Subscribers;
import io.reactivesocket.transport.TransportServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A {@link TransportServer} using local transport. Only {@link LocalClient} instances can connect to this server.
 */
public final class LocalServer implements TransportServer {

    private static final Logger logger = LoggerFactory.getLogger(LocalServer.class);

    private final String name;
    private volatile StartedImpl started;
    /**
     * Active connections, to close when server is shutdown.
     */
    private final ConcurrentLinkedQueue<DuplexConnection> activeConnections = new ConcurrentLinkedQueue<>();

    private LocalServer(String name) {
        this.name = name;
    }

    @Override
    public StartedServer start(ConnectionAcceptor acceptor) {
        synchronized (this) {
            if (started != null) {
                throw new IllegalStateException("Server already started.");
            }
        }
        started = new StartedImpl(acceptor);
        return started;
    }

    public String getName() {
        return name;
    }

    /**
     * Creates a new {@link LocalServer} with the passed {@code name}.
     *
     * @param name Name of this server. This is the unique identifier to connect to this server.
     *
     * @return A new {@link LocalServer} instance.
     */
    public static LocalServer create(String name) {
        final LocalServer toReturn = new LocalServer(name);
        LocalPeersManager.register(toReturn);
        return toReturn;
    }

    void accept(PeerConnector peerConnector) {
        if (null == started) {
            throw new IllegalStateException(String.format("Local server %s not started.", name));
        }

        DuplexConnection serverConn = peerConnector.forServer();
        activeConnections.add(serverConn);
        serverConn.onClose().subscribe(Subscribers.doOnTerminate(() -> activeConnections.remove(serverConn)));
        Px.from(started.acceptor.apply(serverConn))
          .subscribe(Subscribers.cleanup(() -> {
              serverConn.close().subscribe(Subscribers.empty());
          }));
    }

    boolean isActive() {
        return null != started && started.shutdownLatch.getCount() != 0;
    }

    void shutdown() {
        StartedImpl s;
        synchronized (this) {
            s = started;
        }
        if (s != null) {
            s.shutdown();
        }
    }

    private final class StartedImpl implements StartedServer {

        private final ConnectionAcceptor acceptor;
        private final SocketAddress serverAddr;
        private final CountDownLatch shutdownLatch = new CountDownLatch(1);

        private StartedImpl(ConnectionAcceptor acceptor) {
            this.acceptor = acceptor;
            serverAddr = new LocalSocketAddress(name);
        }

        @Override
        public SocketAddress getServerAddress() {
            return serverAddr;
        }

        @Override
        public int getServerPort() {
            return 0; // Local server
        }

        @Override
        public void awaitShutdown() {
            try {
                shutdownLatch.await();
            } catch (InterruptedException e) {
                logger.error("Interrupted while waiting for shutdown.", e);
                Thread.currentThread().interrupt(); // reset the interrupt flag.
            }
        }

        @Override
        public void awaitShutdown(long duration, TimeUnit durationUnit) {
            try {
                shutdownLatch.await(duration, durationUnit);
            } catch (InterruptedException e) {
                logger.error("Interrupted while waiting for shutdown.", e);
                Thread.currentThread().interrupt(); // reset the interrupt flag.
            }
        }

        @Override
        public void shutdown() {
            shutdownLatch.countDown();
            for (DuplexConnection activeConnection : activeConnections) {
                activeConnection.close().subscribe(DefaultSubscriber.defaultInstance());
            }
            LocalPeersManager.unregister(name);
        }
    }
}
