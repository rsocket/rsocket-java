package io.reactivesocket.aeron;

import io.reactivesocket.Completable;
import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.Frame;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.observable.Observer;
import org.reactivestreams.Publisher;
import rx.Scheduler;
import rx.schedulers.Schedulers;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.FragmentAssembler;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.reactivesocket.aeron.Constants.CLIENT_STREAM_ID;
import static io.reactivesocket.aeron.Constants.EMTPY;
import static io.reactivesocket.aeron.Constants.SERVER_STREAM_ID;

/**
 * Created by rroeser on 8/13/15.
 */
public class ReactivesocketAeronClient implements Loggable, AutoCloseable {
    static final ConcurrentHashMap<Integer, Subscription> subscriptions = new ConcurrentHashMap<>();

    static final ConcurrentHashMap<Integer, AeronClientDuplexConnection> connections = new ConcurrentHashMap<>();

    static final ConcurrentHashMap<Integer, CountDownLatch> establishConnectionLatches = new ConcurrentHashMap<>();

    static final ConcurrentHashMap<Integer, Publication> publications = new ConcurrentHashMap<>();

    private ReactiveSocket reactiveSocket;

    private final Aeron aeron;

    private volatile static boolean running = true;

    volatile int sessionId;

    volatile int serverSessionId;

    private static final CountDownLatch shutdownLatch = new CountDownLatch(1);

    private final int port;

    private static final ManyToOneConcurrentArrayQueue<FrameHolder> framesSendQueue;

    private static int mtuLength;

    static {
        Runtime
            .getRuntime()
            .addShutdownHook(new Thread(() -> {
                running = false;

                try {
                    shutdownLatch.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                for (Subscription subscription : subscriptions.values()) {
                    subscription.close();
                }

                for (AeronClientDuplexConnection connection : connections.values()) {
                    connection.close();
                }
            }));

        int queueSize = Integer.getInteger("framesSendQueueSize", 16);
        framesSendQueue = new ManyToOneConcurrentArrayQueue<>(queueSize);
        mtuLength = Integer.getInteger("aeron.mtu.length", 4096);
    }

    private static volatile boolean pollingStarted = false;

    private ReactivesocketAeronClient(String host, String server, int port) {
        this.port = port;

        final Aeron.Context ctx = new Aeron.Context();
        ctx.errorHandler(t -> {
            t.printStackTrace();
        });

        aeron = Aeron.connect(ctx);

        final String channel = "udp://" + host + ":" + port;
        final String subscriptionChannel = "udp://" + server + ":" + port;

        System.out.println("Creating a publication to channel => " + channel);
        Publication publication = aeron.addPublication(channel, SERVER_STREAM_ID);
        publications.putIfAbsent(publication.sessionId(), publication);
        System.out.println("Creating publication => " + publication.toString());
        sessionId = publication.sessionId();

        System.out.println("Created a publication for sessionId => " + sessionId);
        Subscription subscription = subscriptions.get(port);

        if (subscription == null) {
            synchronized (subscriptions) {
                System.out.println("Creating a subscription to channel => " + subscriptionChannel);
                subscription = aeron.addSubscription(subscriptionChannel, CLIENT_STREAM_ID);
                subscriptions.putIfAbsent(port, subscription);
                System.out.println("Subscription created to channel => " + subscriptionChannel);

                if (!pollingStarted) {
                    System.out.println("Polling hasn't started yet - starting polling");

                    final FragmentAssembler fragmentAssembler = new FragmentAssembler(this::fragmentHandler);

                    poll(fragmentAssembler, Schedulers.newThread().createWorker());

                    pollingStarted = true;
                }
            }
        }

        establishConnection(publication, sessionId);

    }

    public static ReactivesocketAeronClient create(String host, String server, int port) {
        return new ReactivesocketAeronClient(host, server, port);
    }

    public static ReactivesocketAeronClient create(String host, String server) {
        return create(host, server, 39790);
    }

    void fragmentHandler(DirectBuffer buffer, int offset, int length, Header header) {
        int messageTypeInt = buffer.getInt(offset);
        MessageType messageType = MessageType.from(messageTypeInt);
        if (messageType == MessageType.FRAME) {
            final AeronClientDuplexConnection connection = connections.get(header.sessionId());
            Observer<Frame> subscriber = connection.getSubscriber();
            final ByteBuffer bytes = ByteBuffer.allocate(length);
            buffer.getBytes(BitUtil.SIZE_OF_INT + offset, bytes, length);
            final Frame frame = Frame.from(bytes);
            subscriber.onNext(frame);
        } else if (messageType == MessageType.ESTABLISH_CONNECTION_RESPONSE) {
            final int ackSessionId = buffer.getInt(offset + BitUtil.SIZE_OF_INT);
            Publication publication = publications.get(ackSessionId);
            System.out.println(String.format("Received establish connection ack for session id => %d", ackSessionId));
            System.out.println("ESTABLISH_CONNECTION_RESPONSE publication => " + publication.toString());
            serverSessionId = header.sessionId();
            final AeronClientDuplexConnection connection =
                connections
                    .computeIfAbsent(serverSessionId, (_p) ->
                        new AeronClientDuplexConnection(publication, framesSendQueue, initialized));

            reactiveSocket =  ReactiveSocket.fromClientConnection(
                connection,
                ConnectionSetupPayload.create("UTF-8", "UTF-8", ConnectionSetupPayload.NO_FLAGS),
                err -> err.printStackTrace());

            reactiveSocket.start(new Completable() {
                @Override
                public void success() {
                    initialized.set(true);
                    System.out.println("SUCCESSSSSS!!!!!!");
                }

                @Override
                public void error(Throwable e) {
                    initialized.set(true);
                    System.out.println("NOOOOOO!!!!!!");
                }
            });


            info("ReactiveSocket connected to Aeron session => " + ackSessionId);
            CountDownLatch latch = establishConnectionLatches.remove(ackSessionId);

            latch.countDown();
            System.out.println("HERE #*#*#*#*#**#*#*#*#*#*");

        } else {
            debug("Unknown message type => " + messageTypeInt);
        }
    }

    AtomicBoolean initialized = new AtomicBoolean(false);

    void poll(FragmentAssembler fragmentAssembler, Scheduler.Worker worker) {
        worker.schedule(() -> {
            while (running) {
                try {

                    // Pool subscriptions
                    Collection<Subscription> subscriptions = ReactivesocketAeronClient.subscriptions.values();
                    if (subscriptions != null) {
                        subscriptions.forEach(subscription ->
                                subscription.poll(fragmentAssembler, Integer.MAX_VALUE)
                        );
                    }

                    ArrayList<FrameHolder> frames = new ArrayList<>(128);

                    // Drain send queue
                    framesSendQueue
                        .drainTo(frames, 128);

                    frames.forEach(fh -> {
                        Frame frame = fh.getFrame();
                        System.out.println("QUEUE FRAME => " + frame.toString());

                        final ByteBuffer byteBuffer = frame.getByteBuffer();
                        final int length = byteBuffer.capacity() + BitUtil.SIZE_OF_INT;

                        // If the length is less the MTU size send the message using tryClaim which does not fragment the message
                        // If the message is larger the the MTU size send it using offer.
                        if (length < mtuLength) {
                            tryClaim(fh.getPublication(), byteBuffer, length);
                        } else {
                            offer(fh.getPublication(), byteBuffer, length);
                        }

                        frame.release();
                    });

                    poll(fragmentAssembler, worker);
                } catch (Throwable t) {
                    t.printStackTrace();
                    error(t.getMessage(), t);
                }
            }

            shutdownLatch.countDown();

        });
    }

    private static final ThreadLocal<BufferClaim> bufferClaims = ThreadLocal.withInitial(BufferClaim::new);

    private static final ThreadLocal<UnsafeBuffer> unsafeBuffers = ThreadLocal.withInitial(() -> new UnsafeBuffer(Constants.EMTPY));

    void offer(Publication publication, ByteBuffer byteBuffer, int length) {
        final byte[] bytes = new byte[length];
        final UnsafeBuffer unsafeBuffer = unsafeBuffers.get();
        unsafeBuffer.wrap(bytes);
        unsafeBuffer.putInt(0, MessageType.FRAME.getEncodedType());
        unsafeBuffer.putBytes(BitUtil.SIZE_OF_INT, byteBuffer, byteBuffer.capacity());
        do {
            final long offer = publication.offer(unsafeBuffer);
            if (offer >= 0) {
                break;
            } else if (Publication.NOT_CONNECTED == offer) {
                throw new RuntimeException("not connected");
            }
        } while(true);

    }

    void tryClaim(Publication publication, ByteBuffer byteBuffer, int length) {
        final BufferClaim bufferClaim = bufferClaims.get();
        do {
            final long offer = publication.tryClaim(length, bufferClaim);
            if (offer >= 0) {
                try {
                    final MutableDirectBuffer buffer = bufferClaim.buffer();
                    final int offset = bufferClaim.offset();
                    buffer.putInt(offset, MessageType.FRAME.getEncodedType());
                    buffer.putBytes(offset + BitUtil.SIZE_OF_INT, byteBuffer, 0, byteBuffer.capacity());
                } finally {
                    bufferClaim.commit();
                }

                break;
            } else if (Publication.NOT_CONNECTED == offer) {
                throw new RuntimeException("not connected");
            }
        } while(true);
    }


    /**
     * Establishes a connection between the client and server. Waits for 30 seconds before throwing a exception.
     */
    void establishConnection(final Publication publication, final int sessionId) {
        try {
            final UnsafeBuffer buffer = new UnsafeBuffer(EMTPY);
            buffer.wrap(new byte[BitUtil.SIZE_OF_INT]);
            buffer.putInt(0, MessageType.ESTABLISH_CONNECTION_REQUEST.getEncodedType());

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

    public Publisher<Payload> requestResponse(Payload payload) {
        return reactiveSocket.requestResponse(payload);
    }

    public Publisher<Void> fireAndForget(Payload payload) {
        return reactiveSocket.fireAndForget(payload);
    }

    public Publisher<Payload> requestStream(Payload payload) {
        return reactiveSocket.requestStream(payload);
    }

    public Publisher<Payload> requestSubscription(Payload payload) {
        return reactiveSocket.requestSubscription(payload);
    }

    @Override
    public void close() throws Exception {
        // First clean up the different maps
        // Remove the AeronDuplexConnection from the connections map
        AeronClientDuplexConnection connection = connections.remove(serverSessionId);

        // This should already be removed but remove it just in case to be safe
        establishConnectionLatches.remove(sessionId);

        // Close the different connections
        closeQuietly(connection);
        closeQuietly(reactiveSocket);
        System.out.println("closing publication => " + publications.get(sessionId).toString());
        Publication publication = publications.remove(sessionId);
        closeQuietly(publication);

    }

    private void closeQuietly(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Throwable t) {
            debug(t.getMessage(), t);
        }
    }
}
