package io.reactivesocket.aeron.client;

import io.reactivesocket.Frame;
import io.reactivesocket.aeron.internal.AeronUtil;
import io.reactivesocket.aeron.internal.Constants;
import io.reactivesocket.aeron.internal.Loggable;
import io.reactivesocket.aeron.internal.MessageType;
import io.reactivesocket.rx.Observer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.ManyToManyConcurrentArrayQueue;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.reactivesocket.aeron.internal.Constants.CONCURRENCY;
import static io.reactivesocket.aeron.internal.Constants.SERVER_STREAM_ID;

public final class AeronClientDuplexConnectionFactory implements Loggable {
    private static final AeronClientDuplexConnectionFactory instance = new AeronClientDuplexConnectionFactory();

    private static ThreadLocal<UnsafeBuffer> buffers = ThreadLocal.withInitial(() -> new UnsafeBuffer(Constants.EMTPY));

    private final ConcurrentSkipListMap<Integer, AeronClientDuplexConnection> connections;

    // TODO - this should be configurable...enough space for 2048 DuplexConnections assuming request(128)
    private final ManyToManyConcurrentArrayQueue<FrameHolder> frameSendQueue = new ManyToManyConcurrentArrayQueue<>(262144);

    private final ConcurrentHashMap<Integer, EstablishConnectionHolder> establishConnectionHolders;

    private final ClientAeronManager manager;

    private AeronClientDuplexConnectionFactory() {
        connections = new ConcurrentSkipListMap<>();
        establishConnectionHolders = new ConcurrentHashMap<>();
        manager = ClientAeronManager.getInstance();

        manager.addClientAction(threadId -> {
            final boolean traceEnabled = isTraceEnabled();
            frameSendQueue
                .drain(fh -> {
                    final Frame frame = fh.getFrame();
                    final ByteBuffer byteBuffer = frame.getByteBuffer();
                    final Publication publication = fh.getPublication();
                    final int length = frame.length() + BitUtil.SIZE_OF_INT;

                    // Can release the FrameHolder at this point as we got everything we need
                    fh.release();

                    AeronUtil
                        .tryClaimOrOffer(publication, (offset, buffer) -> {
                            if (traceEnabled) {
                                trace("Thread Id {} sending Frame => {} on Aeron", threadId, frame.toString());
                            }

                            buffer.putShort(offset, (short) 0);
                            buffer.putShort(offset + BitUtil.SIZE_OF_SHORT, (short) MessageType.FRAME.getEncodedType());
                            buffer.putBytes(offset + BitUtil.SIZE_OF_INT, byteBuffer, frame.offset(), frame.length());
                        }, length);
                });
        });
    }

    public static AeronClientDuplexConnectionFactory getInstance() {
        return instance;
    }

    /**
     * Adds a {@link java.net.SocketAddress} for Aeron to listen for responses on
     *
     * @param socketAddress
     */
    public void addSocketAddressToHandleResponses(SocketAddress socketAddress) {
        if (socketAddress instanceof InetSocketAddress) {
            addUDPSocketAddressToHandleResponses((InetSocketAddress) socketAddress);
        } else {
            throw new RuntimeException("unknown socket address type => " + socketAddress.getClass());
        }
    }

    void addUDPSocketAddressToHandleResponses(InetSocketAddress socketAddress) {
        String serverChannel = "udp://" + socketAddress.getHostName() + ":" + socketAddress.getPort();

        manager.addSubscription(
            serverChannel,
            Constants.CLIENT_STREAM_ID,
            threadId ->
                new ClientAeronManager.ThreadIdAwareFragmentHandler(threadId) {
                    @Override
                    public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
                        fragmentHandler(getThreadId(), buffer, offset, length, header);
                    }
                });
    }

    public Publisher<AeronClientDuplexConnection> createAeronClientDuplexConnection(SocketAddress socketAddress) {
        if (socketAddress instanceof InetSocketAddress) {
            return createUDPConnection((InetSocketAddress) socketAddress);
        } else {
            throw new RuntimeException("unknown socket address type => " + socketAddress.getClass());
        }
    }

    Publisher<AeronClientDuplexConnection> createUDPConnection(InetSocketAddress inetSocketAddress) {
        final String channel = "udp://" + inetSocketAddress.getHostName() + ":" + inetSocketAddress.getPort();
        debug("Creating a publication to channel => {}", channel);
        final Publication publication = manager.getAeron().addPublication(channel, SERVER_STREAM_ID);
        debug("Created a publication with sessionId => {} to channel => {}", publication.sessionId(), channel);

        return subscriber -> {
            EstablishConnectionHolder establishConnectionHolder = new EstablishConnectionHolder(publication, subscriber);
            establishConnectionHolders.putIfAbsent(publication.sessionId(), establishConnectionHolder);

            establishConnection(publication);
        };
    }

    /**
     * Establishes a connection between the client and server. Waits for 30 seconds before throwing a exception.
     */
    void establishConnection(final Publication publication) {
        final int sessionId = publication.sessionId();

        debug("Establishing connection for channel => {}, stream id => {}",
            publication.channel(),
            publication.sessionId());

        UnsafeBuffer buffer = buffers.get();
        buffer.wrap(new byte[BitUtil.SIZE_OF_INT]);
        buffer.putShort(0, (short) 0);
        buffer.putShort(BitUtil.SIZE_OF_SHORT, (short) MessageType.ESTABLISH_CONNECTION_REQUEST.getEncodedType());

        long offer = -1;
        final long start = System.nanoTime();
        for (;;) {
            final long current = System.nanoTime();
            if ((current - start) > TimeUnit.SECONDS.toNanos(30)) {
                throw new RuntimeException("Timed out waiting to establish connection for session id => " + sessionId);
            }

            if (offer < 0) {
                offer = publication.offer(buffer);
            } else {
                break;
            }

        }

    }

    void fragmentHandler(int threadId, DirectBuffer buffer, int offset, int length, Header header) {
        try {
            short messageCount = buffer.getShort(offset);
            short messageTypeInt = buffer.getShort(offset + BitUtil.SIZE_OF_SHORT);
            final int currentThreadId = Math.abs(messageCount % CONCURRENCY);

            if (currentThreadId != threadId) {
                return;
            }

            final MessageType messageType = MessageType.from(messageTypeInt);
            if (messageType == MessageType.FRAME) {
                AeronClientDuplexConnection aeronClientDuplexConnection = connections.get(header.sessionId());
                if (aeronClientDuplexConnection != null) {
                    CopyOnWriteArrayList<Observer<Frame>> subjects = aeronClientDuplexConnection.getSubjects();
                    if (!subjects.isEmpty()) {
                        //TODO think about how to recycle these, hard because could be handed to another thread I think?
                        final ByteBuffer bytes = ByteBuffer.allocate(length);
                        buffer.getBytes(BitUtil.SIZE_OF_INT + offset, bytes, length);
                        final Frame frame = Frame.from(bytes);
                        int i = 0;
                        final int size = subjects.size();
                        do {
                            Observer<Frame> frameObserver = subjects.get(i);
                            frameObserver.onNext(frame);

                            i++;
                        } while (i < size);
                    }
                } else {
                    debug("no connection found for Aeron Session Id {}", header.sessionId());
                }
            } else if (messageType == MessageType.ESTABLISH_CONNECTION_RESPONSE) {
                final int ackSessionId = buffer.getInt(offset + BitUtil.SIZE_OF_INT);
                EstablishConnectionHolder establishConnectionHolder = establishConnectionHolders.remove(ackSessionId);
                if (establishConnectionHolder != null) {
                    try {
                        AeronClientDuplexConnection aeronClientDuplexConnection
                            = new AeronClientDuplexConnection(establishConnectionHolder.getPublication(), frameSendQueue, new Consumer<Publication>() {
                            @Override
                            public void accept(Publication publication) {
                                connections.remove(publication.sessionId());

                                // Send a message to the server that the connection is closed and that it needs to clean-up resources on it's side
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
                        });

                        connections.put(header.sessionId(), aeronClientDuplexConnection);

                        establishConnectionHolder.getSubscriber().onNext(aeronClientDuplexConnection);
                        establishConnectionHolder.getSubscriber().onComplete();

                        debug("Connection established for channel => {}, stream id => {}",
                            establishConnectionHolder.getPublication().channel(),
                            establishConnectionHolder.getPublication().sessionId());
                    } catch (Throwable t) {
                        establishConnectionHolder.getSubscriber().onError(t);
                    }
                }
            } else {
                debug("Unknown message type => " + messageTypeInt);
            }
        } catch (Throwable t) {
            error("error handling framement", t);
        }
    }

    /*
     * Inner Classes
     */
    class EstablishConnectionHolder  {
        private Publication publication;
        private Subscriber<? super AeronClientDuplexConnection> subscriber;

        public EstablishConnectionHolder(Publication publication, Subscriber<? super AeronClientDuplexConnection> subscriber) {
            this.publication = publication;
            this.subscriber = subscriber;
        }

        public Publication getPublication() {
            return publication;
        }

        public Subscriber<? super AeronClientDuplexConnection> getSubscriber() {
            return subscriber;
        }
    }
}
