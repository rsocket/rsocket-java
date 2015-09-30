package io.reactivesocket.aeron.client;

import io.reactivesocket.Frame;
import io.reactivesocket.aeron.internal.AeronUtil;
import io.reactivesocket.aeron.internal.Constants;
import io.reactivesocket.aeron.internal.Loggable;
import io.reactivesocket.aeron.internal.MessageType;
import io.reactivesocket.rx.Observer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.ManyToOneConcurrentArrayQueue;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static io.reactivesocket.aeron.internal.Constants.CONCURRENCY;
import static io.reactivesocket.aeron.internal.Constants.SERVER_STREAM_ID;

public final class AeronClientDuplexConnectionFactory implements Loggable {
    private static final AeronClientDuplexConnectionFactory instance = new AeronClientDuplexConnectionFactory();

    // Aeron Publication Session Id to ReactiveSocket DuplexConnection implementation
    private final ConcurrentSkipListMap<Integer, AeronClientDuplexConnection> connections;

    // TODO - this should be configurable...enough space for 2048 DuplexConnections assuming request(128)
    private final ManyToOneConcurrentArrayQueue<FrameHolder> frameSendQueue = new ManyToOneConcurrentArrayQueue<>(262144);

    private final ConcurrentHashMap<Integer, EstablishConnectionSubscriber> establishConnectionSubscribers;

    private final ClientAeronManager manager;

    private AeronClientDuplexConnectionFactory() {
        connections = new ConcurrentSkipListMap<>();
        establishConnectionSubscribers = new ConcurrentHashMap<>();
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
                                trace("Thread Id {} sending Frame => {} on Aeron", threadId,  frame.toString());
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
     * Adds a {@link java.net.SocketAddress} for Aeron to listen to Responses on
     *
     * @param socketAddress
     */
    public void addSocketAddressToHandleResponses(SocketAddress socketAddress) {
        if (socketAddress instanceof InetSocketAddress) {
            addUDPSocketAddressToHandleResponses((InetSocketAddress) socketAddress);
        }
        throw new RuntimeException("unknow socket address type => " + socketAddress.getClass());
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
        }

        throw new RuntimeException("unknown socket address type => " + socketAddress.getClass());
    }

    Publisher<AeronClientDuplexConnection> createUDPConnection(InetSocketAddress inetSocketAddress) {
        Publisher<AeronClientDuplexConnection> publisher = subscriber -> {
            final String channel = "udp://" + inetSocketAddress.getHostName() + ":" + inetSocketAddress.getPort();
            debug("Creating a publication to channel => {}", channel);
            Publication publication = manager.getAeron().addPublication(channel, SERVER_STREAM_ID);
            debug("Created a publication with sessionId => {} to channel => {}", publication.sessionId(), channel);
            EstablishConnectionSubscriber establishConnectionSubscriber = new EstablishConnectionSubscriber(publication, subscriber);
            establishConnectionSubscribers.put(publication.sessionId(), establishConnectionSubscriber);
        };

        return publisher;
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
                EstablishConnectionSubscriber establishConnectionSubscriber = establishConnectionSubscribers.get(ackSessionId);
                if (establishConnectionSubscribers != null) {
                    try {
                        AeronClientDuplexConnection aeronClientDuplexConnection
                            = new AeronClientDuplexConnection(establishConnectionSubscriber.getPublication(), frameSendQueue, new Consumer<Publication>() {
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
                        establishConnectionSubscriber.onNext(aeronClientDuplexConnection);
                        establishConnectionSubscriber.onComplete();
                    } catch (Throwable t) {
                        establishConnectionSubscriber.onError(t);
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
    class EstablishConnectionSubscriber implements Subscriber<AeronClientDuplexConnection> {
        private Publication publication;
        private Subscriber<? super AeronClientDuplexConnection> child;

        public EstablishConnectionSubscriber(Publication publication, Subscriber<? super AeronClientDuplexConnection> child) {
            this.publication = publication;
            this.child = child;
        }

        public Publication getPublication() {
            return publication;
        }

        @Override
        public void onSubscribe(Subscription s) {
            s.request(Long.MAX_VALUE);
            establishConnectionSubscribers.put(publication.sessionId(), this);
        }

        @Override
        public void onNext(AeronClientDuplexConnection aeronClientDuplexConnection) {
            onNext(aeronClientDuplexConnection);
        }

        @Override
        public void onError(Throwable t) {
            onError(t);
        }

        @Override
        public void onComplete() {
            child.onComplete();
        }
    }
}
