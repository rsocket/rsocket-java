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
package io.reactivesocket.aeron.client;

import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.Frame;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.aeron.internal.AeronUtil;
import io.reactivesocket.aeron.internal.Loggable;
import io.reactivesocket.aeron.internal.MessageType;
import io.reactivesocket.aeron.internal.concurrent.ManyToManyConcurrentArrayQueue;
import io.reactivesocket.rx.Observer;
import org.reactivestreams.Publisher;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static io.reactivesocket.aeron.internal.Constants.CLIENT_STREAM_ID;
import static io.reactivesocket.aeron.internal.Constants.CONCURRENCY;
import static io.reactivesocket.aeron.internal.Constants.EMTPY;
import static io.reactivesocket.aeron.internal.Constants.SERVER_STREAM_ID;

/**
 * Class that exposes ReactiveSocket over Aeron
 *
 */
public class ReactiveSocketAeronClient implements Loggable, AutoCloseable {

    static final ConcurrentSkipListMap<Integer, AeronClientDuplexConnection> connections = new ConcurrentSkipListMap<>();

    static final ConcurrentHashMap<Integer, CountDownLatch> establishConnectionLatches = new ConcurrentHashMap<>();

    static final ConcurrentHashMap<Integer, Publication> publications = new ConcurrentHashMap<>();

    static final ConcurrentHashMap<Integer, ReactiveSocket> reactiveSockets = new ConcurrentHashMap<>();

    static ClientAeronManager manager = ClientAeronManager.getInstance();

    final int sessionId;

    //For Test
    ReactiveSocketAeronClient() {
        sessionId = -1;
    }

    /**
     * Creates a new ReactivesocketAeronClient
     *
     * @param host the host name that client is listening on
     * @param server the host of the server that client will send data too
     * @param port the port to send and receive data on
     */
    private ReactiveSocketAeronClient(String host, String server, int port) {
        final String channel = "udp://" + host + ":" + port;
        final String subscriptionChannel = "udp://" + server + ":" + port;

        debug("Creating a publication to channel => {}", channel);
        Publication publication = manager.getAeron().addPublication(channel, SERVER_STREAM_ID);
        publications.putIfAbsent(publication.sessionId(), publication);
        sessionId = publication.sessionId();
        debug("Created a publication with sessionId => {}", sessionId);

        manager.addSubscription(subscriptionChannel, CLIENT_STREAM_ID, (Integer threadId) ->
                new ClientAeronManager.ThreadIdAwareFragmentHandler(threadId) {
                    @Override
                    public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
                        fragmentHandler(getThreadId(), buffer, offset, length, header);
                    }
                }
        );

        establishConnection(publication, sessionId);
    }

    void fragmentHandler(int threadId, DirectBuffer buffer, int offset, int length, Header header) {
        try {
            short messageCount = buffer.getShort(offset);
            short messageTypeInt  = buffer.getShort(offset + BitUtil.SIZE_OF_SHORT);
            final int currentThreadId = Math.abs(messageCount % CONCURRENCY);

            if (currentThreadId != threadId) {
                return;
            }

            final MessageType messageType = MessageType.from(messageTypeInt);
            if (messageType == MessageType.FRAME) {
                final AeronClientDuplexConnection connection = connections.get(header.sessionId());
                if (connection != null) {
                    final List<? extends Observer<Frame>> subscribers = connection.getSubscriber();
                    if (!subscribers.isEmpty()) {
                        //TODO think about how to recycle these, hard because could be handed to another thread I think?
                        final ByteBuffer bytes = ByteBuffer.allocate(length);
                        buffer.getBytes(BitUtil.SIZE_OF_INT + offset, bytes, length);
                        final Frame frame = Frame.from(bytes);
                        int i = 0;
                        final int size = subscribers.size();
                        do {
                            subscribers.get(i).onNext(frame);
                            i++;
                        } while (i < size);
                    }
                } else {
                    debug("no connection found for Aeron Session Id {}", sessionId);
                }
            } else if (messageType == MessageType.ESTABLISH_CONNECTION_RESPONSE) {
                final int ackSessionId = buffer.getInt(offset + BitUtil.SIZE_OF_INT);

                CountDownLatch latch = establishConnectionLatches.remove(ackSessionId);

                if (latch == null) {
                    return;
                }

                Publication publication = publications.get(ackSessionId);
                final int serverSessionId = header.sessionId();
                debug("Received establish connection ack for session id => {}, and server session id => {}", ackSessionId, serverSessionId);
                final AeronClientDuplexConnection connection =
                    connections
                        .computeIfAbsent(serverSessionId, (_p) ->
                            new AeronClientDuplexConnection(publication));

                ReactiveSocket reactiveSocket = ReactiveSocket.fromClientConnection(
                    connection,
                    ConnectionSetupPayload.create("UTF-8", "UTF-8", ConnectionSetupPayload.NO_FLAGS),
                    err -> err.printStackTrace());

                reactiveSocket.startAndWait();

                reactiveSockets.putIfAbsent(ackSessionId, reactiveSocket);

                ReactiveSocketAeronClientAction clientAction
                    = new ReactiveSocketAeronClientAction(ackSessionId, reactiveSocket, connection);

                manager
                    .find(header.sessionId())
                    .ifPresent(sg ->
                        sg
                            .getClientActions()
                            .add(clientAction)
                    );

                latch.countDown();

                debug("ReactiveSocket connected to Aeron session => " + ackSessionId);
            } else {
                debug("Unknown message type => " + messageTypeInt);
            }
        } catch (Throwable t) {
            error("error handling framement", t);
        }
    }

    static class ReactiveSocketAeronClientAction extends ClientAeronManager.ClientAction implements Loggable {
        private final ReactiveSocket reactiveSocket;
        private final AeronClientDuplexConnection connection;

        public ReactiveSocketAeronClientAction(int sessionId, ReactiveSocket reactiveSocket, AeronClientDuplexConnection connection) {
            super(sessionId);
            this.reactiveSocket = reactiveSocket;
            this.connection = connection;
        }

        @Override
        void call(int threadId) {
            final boolean traceEnabled = isTraceEnabled();

            //final int calculatedThreadId = Math.abs(connection.getConnectionId() % CONCURRENCY);

            //if (traceEnabled) {
             //   trace("processing request for thread id => {}, caculcatedThreadId => {}", threadId, calculatedThreadId);
                //LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
            //}

            //if (threadId == calculatedThreadId) {
                ManyToManyConcurrentArrayQueue<FrameHolder> framesSendQueue = connection.getFramesSendQueue();

                /*
                if (traceEnabled && !framesSendQueue.isEmpty()) {
                    if (!framesSendQueue.isEmpty()) {
                        trace("Thread Id {} and connection Id {} draining queue", connection.getConnectionId());
                    } else {
                        trace("Thread Id {} and connection Id {} found empty queue", connection.getConnectionId());
                    }
                } */

                framesSendQueue
                    .drain((FrameHolder fh) -> {
                        try {
                            Frame frame = fh.getFrame();

                            final ByteBuffer byteBuffer = frame.getByteBuffer();
                            final int length = byteBuffer.capacity() + BitUtil.SIZE_OF_INT;
                            Publication publication = publications.get(id);
                            AeronUtil.tryClaimOrOffer(publication, (offset, buffer) -> {

                                if (traceEnabled) {
                                    trace("Thread Id {} and connection Id {} sending Frame => {} on Aeron", threadId, connection.getConnectionId(), frame.toString());
                                }

                                buffer.putShort(offset, (short) 0);
                                buffer.putShort(offset + BitUtil.SIZE_OF_SHORT, (short) MessageType.FRAME.getEncodedType());
                                buffer.putBytes(offset + BitUtil.SIZE_OF_INT, byteBuffer, frame.offset(), frame.length());
                            }, length);

                            fh.release();

                        } catch (Throwable t) {
                            fh.release();
                            error("error draining send frame queue", t);
                        }
                    });
            //}
        }

        @Override
        public void close() throws Exception {
            manager
                .find(id)
                .ifPresent(sg -> sg.getClientActions().remove(this));

            reactiveSocket.close();
            connection.close();
        }
    }

    /**
     * Establishes a connection between the client and server. Waits for 30 seconds before throwing a exception.
     */
    void establishConnection(final Publication publication, final int sessionId) {

        try {
            final UnsafeBuffer buffer = new UnsafeBuffer(EMTPY);
            buffer.wrap(new byte[BitUtil.SIZE_OF_INT]);
            buffer.putShort(0, (short) 0);
            buffer.putShort(BitUtil.SIZE_OF_SHORT, (short) MessageType.ESTABLISH_CONNECTION_REQUEST.getEncodedType());

            CountDownLatch latch = new CountDownLatch(1);
            establishConnectionLatches.put(sessionId, latch);

            long offer = -1;
            final long start = System.nanoTime();
            for (;;) {
                final long current = System.nanoTime();
                if ((current - start) > TimeUnit.SECONDS.toNanos(30)) {
                    throw new RuntimeException("Timed out waiting to establish connection for session id => " + sessionId);
                }

                if (offer < 0) {
                    offer = publication.offer(buffer);
                }
                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));

                if (latch.getCount() == 0) {
                    break;
                }
            }

           debug("Connection established for channel => {}, stream id => {}",
                publication.channel(),
                publication.sessionId());
        } finally {
            establishConnectionLatches.remove(sessionId);
        }

    }

    @Override
    public void close() throws Exception {
        manager.removeClientAction(sessionId);

        Publication publication = publications.remove(sessionId);

        if (publication != null) {
            try {

                AeronUtil.tryClaimOrOffer(publication, (offset, buffer) -> {
                    buffer.putShort(offset, (short) 0);
                    buffer.putShort(offset + BitUtil.SIZE_OF_SHORT, (short) MessageType.CONNECTION_DISCONNECT.getEncodedType());
                }, BitUtil.SIZE_OF_INT);
            } catch (Throwable t) {
                debug("error closing  publication with session id => {}", publication.sessionId());
            }
            publication.close();
        }
    }

    /*
     * ReactiveSocket methods
     */
    public Publisher<Payload> requestResponse(Payload payload) {
        ReactiveSocket reactiveSocket = reactiveSockets.get(sessionId);
        return reactiveSocket.requestResponse(payload);
    }

    public Publisher<Void> fireAndForget(Payload payload) {
        ReactiveSocket reactiveSocket = reactiveSockets.get(sessionId);
        return reactiveSocket.fireAndForget(payload);
    }

    public Publisher<Payload> requestStream(Payload payload) {
        ReactiveSocket reactiveSocket = reactiveSockets.get(sessionId);
        return reactiveSocket.requestStream(payload);
    }

    public Publisher<Payload> requestSubscription(Payload payload) {
        ReactiveSocket reactiveSocket = reactiveSockets.get(sessionId);
        return reactiveSocket.requestSubscription(payload);
    }

    /*
     * Factory Methods
     */
    public static ReactiveSocketAeronClient create(String host, String server, int port) {
        return new ReactiveSocketAeronClient(host, server, port);
    }

    public static ReactiveSocketAeronClient create(String host, String server) {
        return create(host, server, 39790);
    }

}
