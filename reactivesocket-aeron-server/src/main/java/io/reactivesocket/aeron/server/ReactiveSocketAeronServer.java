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
package io.reactivesocket.aeron.server;

import io.reactivesocket.ConnectionSetupHandler;
import io.reactivesocket.Frame;
import io.reactivesocket.LeaseGovernor;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.aeron.internal.Loggable;
import io.reactivesocket.aeron.internal.MessageType;
import io.reactivesocket.rx.Observer;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.FragmentAssembler;
import uk.co.real_logic.aeron.Image;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.agrona.DirectBuffer;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.reactivesocket.aeron.internal.Constants.CLIENT_STREAM_ID;
import static io.reactivesocket.aeron.internal.Constants.SERVER_STREAM_ID;

public class ReactiveSocketAeronServer implements AutoCloseable, Loggable {
    private final int port;

    private final ConcurrentHashMap<Integer, AeronServerDuplexConnection> connections = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<Integer, ReactiveSocket> sockets = new ConcurrentHashMap<>();

    private final Subscription subscription;

    private final ConnectionSetupHandler connectionSetupHandler;

    private final LeaseGovernor leaseGovernor;

    private static final ServerAeronManager manager = ServerAeronManager.getInstance();

    private ReactiveSocketAeronServer(String host, int port, ConnectionSetupHandler connectionSetupHandler, LeaseGovernor leaseGovernor) {
        this.port = port;
        this.connectionSetupHandler = connectionSetupHandler;
        this.leaseGovernor = leaseGovernor;

        manager.addAvailableImageHander(this::availableImageHandler);
        manager.addUnavailableImageHandler(this::unavailableImage);

        Aeron aeron = manager.getAeron();

        final String serverChannel =  "udp://" + host + ":" + port;
        info("Starting new ReactiveSocketAeronServer on channel {}", serverChannel);
        subscription = aeron.addSubscription(serverChannel, SERVER_STREAM_ID);

        FragmentAssembler fragmentAssembler = new FragmentAssembler(this::fragmentHandler);
        manager.addSubscription(subscription, fragmentAssembler);


    }

    void fragmentHandler(DirectBuffer buffer, int offset, int length, Header header) {
            final int sessionId = header.sessionId();

            short messageTypeInt = buffer.getShort(offset + BitUtil.SIZE_OF_SHORT);
            MessageType type = MessageType.from(messageTypeInt);

            if (MessageType.FRAME == type) {
                AeronServerDuplexConnection connection = connections.get(sessionId);
                if (connection != null) {
                    List<? extends Observer<Frame>> subscribers = connection.getSubscriber();
                    final Frame frame = Frame.from(buffer, BitUtil.SIZE_OF_INT + offset, length);

                    if (isTraceEnabled()) {
                        trace("server received frame payload {} on session id {}", frame.getData(), sessionId);
                    }

                    subscribers.forEach(s -> {
                        try {
                            s.onNext(frame);
                        } catch (Throwable t) {
                            s.onError(t);
                        }
                    });
                }
            } else if (MessageType.ESTABLISH_CONNECTION_REQUEST == type) {
                final long start = System.nanoTime();
                AeronServerDuplexConnection connection = null;
                debug("Looking for an AeronServerDuplexConnection connection to ack establish connection for session id => {}", sessionId);
                while (connection == null) {
                    final long current = System.nanoTime();

                    if (current - start > TimeUnit.SECONDS.toNanos(30)) {
                        throw new RuntimeException("unable to find connection to ack establish connection for session id => " + sessionId);
                    }

                    connection = connections.get(sessionId);
                }
                debug("Found a connection to ack establish connection for session id => {}", sessionId);
                connection.ackEstablishConnection(sessionId);
            } else if (MessageType.CONNECTION_DISCONNECT == type) {
                closeReactiveSocket(sessionId);
            }

    }

    void availableImageHandler(Image image, Subscription subscription, long joiningPosition, String sourceIdentity) {
        final int streamId = subscription.streamId();
        final int sessionId = image.sessionId();
        if (SERVER_STREAM_ID == streamId) {
            debug("Handling new image for session id => {} and stream id => {}", streamId, sessionId);
            final AeronServerDuplexConnection connection = connections.computeIfAbsent(sessionId, (_s) -> {
                final String responseChannel = "udp://" + sourceIdentity.substring(0, sourceIdentity.indexOf(':')) + ":" + port;
                Publication publication = manager.getAeron().addPublication(responseChannel, CLIENT_STREAM_ID);
                int responseSessionId = publication.sessionId();
                debug("Creating new connection for responseChannel => {}, streamId => {}, and sessionId => {}", responseChannel, streamId, responseSessionId);
                return new AeronServerDuplexConnection(publication);
            });
            debug("Accepting ReactiveSocket connection");
            ReactiveSocket socket = ReactiveSocket.fromServerConnection(
                connection,
                connectionSetupHandler,
                leaseGovernor,
                new Consumer<Throwable>() {
                    @Override
                    public void accept(Throwable throwable) {
                        error(String.format("Error creating ReactiveSocket for Aeron session id => %d and stream id => %d", streamId, sessionId), throwable);
                    }
                });

            sockets.put(sessionId, socket);

            socket.startAndWait();
        } else {
            debug("Unsupported stream id {}", streamId);
        }
    }

    void unavailableImage(Image image, Subscription subscription, long position) {
        closeReactiveSocket(image.sessionId());
    }

    private void closeReactiveSocket(int sessionId) {
        debug("closing connection for session id => " + sessionId);
        ReactiveSocket socket = sockets.remove(sessionId);
        connections.remove(sessionId);

        try {
            socket.close();
        } catch (Throwable t) {
            error("error closing socket for session id => " + sessionId, t);
        }
    }

    public boolean hasConnections() {
        return !connections.isEmpty();
    }

    @Override
    public void close() throws Exception {
        manager.removeSubscription(subscription);
    }

    /*
     * Factory Methods
     */
    public static ReactiveSocketAeronServer create(String host, int port, ConnectionSetupHandler connectionSetupHandler, LeaseGovernor leaseGovernor) {
        return new ReactiveSocketAeronServer(host, port, connectionSetupHandler, leaseGovernor);
    }

    public static ReactiveSocketAeronServer create(int port, ConnectionSetupHandler connectionSetupHandler, LeaseGovernor leaseGovernor) {
        return create("127.0.0.1", port, connectionSetupHandler, leaseGovernor);
    }

    public static ReactiveSocketAeronServer create(ConnectionSetupHandler connectionSetupHandler, LeaseGovernor leaseGovernor) {
        return create(39790, connectionSetupHandler, leaseGovernor);
    }

    public static ReactiveSocketAeronServer create(String host, int port, ConnectionSetupHandler connectionSetupHandler) {
        return new ReactiveSocketAeronServer(host, port, connectionSetupHandler, LeaseGovernor.NULL_LEASE_GOVERNOR);
    }

    public static ReactiveSocketAeronServer create(int port, ConnectionSetupHandler connectionSetupHandler) {
        return create("localhost", port, connectionSetupHandler, LeaseGovernor.NULL_LEASE_GOVERNOR);
    }

    public static ReactiveSocketAeronServer create(ConnectionSetupHandler connectionSetupHandler) {
        return create(39790, connectionSetupHandler, LeaseGovernor.NULL_LEASE_GOVERNOR);
    }

}
