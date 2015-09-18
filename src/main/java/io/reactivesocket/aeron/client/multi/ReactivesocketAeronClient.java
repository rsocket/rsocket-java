package io.reactivesocket.aeron.client.multi;

import io.reactivesocket.Completable;
import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.Frame;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.aeron.internal.Constants;
import io.reactivesocket.aeron.internal.Loggable;
import io.reactivesocket.aeron.internal.MessageType;
import io.reactivesocket.aeron.internal.concurrent.ManyToManyConcurrentArrayQueue;
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
import uk.co.real_logic.agrona.LangUtil;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import static io.reactivesocket.aeron.internal.Constants.CLIENT_STREAM_ID;
import static io.reactivesocket.aeron.internal.Constants.EMTPY;
import static io.reactivesocket.aeron.internal.Constants.SERVER_STREAM_ID;


/**
 * Created by rroeser on 9/16/15.
 */
public class ReactivesocketAeronClient  implements Loggable, AutoCloseable {
    static final ArrayList<SubscriptionGroup> SUBSCRIPTION_GROUPS = new ArrayList<>();

    private class SubscriptionGroup {
        String channel;
        Subscription[] subscriptions;
    }

    static final ConcurrentHashMap<Integer, AeronClientDuplexConnection> connections = new ConcurrentHashMap<>();

    static final ConcurrentHashMap<Integer, CountDownLatch> establishConnectionLatches = new ConcurrentHashMap<>();

    static final ConcurrentHashMap<Integer, Publication> publications = new ConcurrentHashMap<>();

    static final ConcurrentHashMap<Integer, ReactiveSocket> reactiveSockets = new ConcurrentHashMap<>();

    private final Aeron aeron;

    private volatile static boolean running = true;

    volatile int sessionId;

    volatile int serverSessionId;

    private static final CountDownLatch shutdownLatch = new CountDownLatch(1);

    private final int port;

    private static int mtuLength;

    private static final int NUM_PROCESSORS = Runtime.getRuntime().availableProcessors() / 2;

    private static Scheduler.Worker[] workers;

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

                for (SubscriptionGroup subscriptionGroup : SUBSCRIPTION_GROUPS) {
                    for (Subscription subscription : subscriptionGroup.subscriptions) {
                        subscription.close();
                    }
                }

                for (AeronClientDuplexConnection connection : connections.values()) {
                    connection.close();
                }
            }));

        mtuLength = Integer.getInteger("aeron.mtu.length", 4096);
        workers = new Scheduler.Worker[NUM_PROCESSORS];

        for (int i = 0; i < NUM_PROCESSORS; i++) {
            workers[i] = Schedulers.computation().createWorker();
        }
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
        synchronized (SUBSCRIPTION_GROUPS) {
            boolean found = SUBSCRIPTION_GROUPS
                .stream()
                .anyMatch(sg -> subscriptionChannel.equals(sg.channel));
            if (!found) {
                SubscriptionGroup subscriptionGroup = new SubscriptionGroup();
                subscriptionGroup.subscriptions = new Subscription[NUM_PROCESSORS];
                for (int i = 0; i < NUM_PROCESSORS; i++) {
                    System.out.println("Creating a subscription to channel => " + subscriptionChannel + ", and processing => " + i);
                    subscriptionGroup.subscriptions[i] = aeron.addSubscription(subscriptionChannel, CLIENT_STREAM_ID);
                    System.out.println("Subscription created to channel => " + subscriptionChannel + ", and processing => " + i);
                }
                SUBSCRIPTION_GROUPS.add(subscriptionGroup);
            }
        }

        if (!pollingStarted) {
            System.out.println("Polling hasn't started yet - starting "
                + Runtime.getRuntime().availableProcessors()
                + " pollers");
            CyclicBarrier startBarrier = new CyclicBarrier(NUM_PROCESSORS + 1);
            for (int i = 0; i < NUM_PROCESSORS; i++) {
                System.out.println("Starting "
                    + i
                    + " poller");
                poll(i, Schedulers.computation().createWorker(), startBarrier);
            }

            try {
                startBarrier.await(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                LangUtil.rethrowUnchecked(e);
            }

            pollingStarted = true;
        }

        establishConnection(publication, sessionId);

    }

    public static ReactivesocketAeronClient create(String host, String server, int port) {
        return new ReactivesocketAeronClient(host, server, port);
    }

    public static ReactivesocketAeronClient create(String host, String server) {
        return create(host, server, 39790);
    }

    static final AtomicLong atomicLong = new AtomicLong();

    void fragmentHandler(int magicNumber, DirectBuffer buffer, int offset, int length, Header header) {

        try {
            short messageCount = buffer.getShort(offset);
            short messageTypeInt  = buffer.getShort(offset + BitUtil.SIZE_OF_SHORT);
            final int currentMagic = Math.abs(messageCount % NUM_PROCESSORS);
/*
            StringBuilder sb = new StringBuilder();

            sb.append(Thread.currentThread() + " messageTypeInt => " + messageTypeInt).append('\n');
            sb.append(Thread.currentThread() + " messageCount => " + messageCount).append('\n');
            sb.append(Thread.currentThread() + " message type => " + messageType).append('\n');

            System.out.println(sb.toString());*/

            //LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
            if (currentMagic != magicNumber) {
                return;
            }

            final MessageType messageType = MessageType.from(messageTypeInt);
            if (messageType == MessageType.FRAME) {
                final AeronClientDuplexConnection connection = connections.get(header.sessionId());
                Observer<Frame> subscriber = connection.getSubscriber();
                final ByteBuffer bytes = ByteBuffer.allocate(length);
                buffer.getBytes(BitUtil.SIZE_OF_INT + offset, bytes, length);
                final Frame frame = Frame.from(bytes);
                //System.out.println("$$$$ CLIENT GOT => " + frame.toString() + " - " + atomicLong.getAndIncrement());
                subscriber.onNext(frame);
            } else if (messageType == MessageType.ESTABLISH_CONNECTION_RESPONSE) {
                final int ackSessionId = buffer.getInt(offset + BitUtil.SIZE_OF_INT);

                CountDownLatch latch = establishConnectionLatches.remove(ackSessionId);

                if (latch == null) {
                    System.out.println(Thread.currentThread() + " => null");
                    return;
                }

                Publication publication = publications.get(ackSessionId);
                serverSessionId = header.sessionId();
                System.out.println(String.format("Received establish connection ack for session id => %d, and server session id => %d", ackSessionId, serverSessionId));
                final AeronClientDuplexConnection connection =
                    connections
                        .computeIfAbsent(serverSessionId, (_p) ->
                            new AeronClientDuplexConnection(publication));

                ReactiveSocket reactiveSocket = ReactiveSocket.fromClientConnection(
                    connection,
                    ConnectionSetupPayload.create("UTF-8", "UTF-8", ConnectionSetupPayload.NO_FLAGS),
                    err -> err.printStackTrace());

                reactiveSocket.start(new Completable() {
                    @Override
                    public void success() {

                    }

                    @Override
                    public void error(Throwable e) {

                    }
                });

                reactiveSockets.putIfAbsent(ackSessionId, reactiveSocket);

                latch.countDown();

                System.out.println("ReactiveSocket connected to Aeron session => " + ackSessionId);
            } else {
                debug("Unknown message type => " + messageTypeInt);
            }
        } catch (Throwable t) {
            System.out.println("ERROR fragmentHandler");
            t.printStackTrace();
            error("error handling framement", t);
        }
    }

    void poll(final int magicNumber, final Scheduler.Worker worker, CyclicBarrier startBarrier) {
        worker.schedulePeriodically(() -> {
            if (startBarrier != null && !pollingStarted) {
                try {
                    System.out.println("Waiting... " + magicNumber);
                    startBarrier.await(30, TimeUnit.SECONDS);
                    System.out.println("We have waited... " + magicNumber);
                } catch (Exception e) {
                    LangUtil.rethrowUnchecked(e);
                }
            }

            if (running) {
                try {
                    final Collection<AeronClientDuplexConnection> values = connections.values();

                    if (values != null) {
                        values.forEach(connection -> {
                            ManyToManyConcurrentArrayQueue<FrameHolder> framesSendQueue = connection.getFramesSendQueue();
                            framesSendQueue
                                .drain((FrameHolder fh) -> {
                                    try {
                                        Frame frame = fh.getFrame();
                                        final ByteBuffer byteBuffer = frame.getByteBuffer();
                                        final int length = byteBuffer.capacity() + BitUtil.SIZE_OF_INT;

                                        // If the length is less the MTU size send the message using tryClaim which does not fragment the message
                                        // If the message is larger the the MTU size send it using offer.
                                        if (length < mtuLength) {
                                            tryClaim(fh.getPublication(), byteBuffer, length);
                                        } else {
                                            offer(fh.getPublication(), byteBuffer, length);
                                        }
                                    } catch (Throwable t) {
                                        error("error draining send frame queue", t);
                                    } finally {
                                        fh.release();
                                    }
                                });
                        });
                    }


                    try {
                        final FragmentAssembler fragmentAssembler = new FragmentAssembler(
                            (DirectBuffer buffer, int offset, int length, Header header) ->
                                fragmentHandler(magicNumber, buffer, offset, length, header));

                            //System.out.println("processing subscriptions => " + magicNumber);
                            //LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
                            SUBSCRIPTION_GROUPS
                                .forEach(subscriptionGroup -> {
                                    //System.out.println("processing subscriptions in foreach => " + magicNumber);
                                    //LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
                                    Subscription subscription = subscriptionGroup.subscriptions[magicNumber];
                                    subscription.poll(fragmentAssembler, Integer.MAX_VALUE);
                                });
                    } catch (Throwable t) {
                        t.printStackTrace();
                        error("error polling aeron subscription", t);
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                }

            } else {
                shutdownLatch.countDown();
            }

        }, 0, 1, TimeUnit.NANOSECONDS);
    }

    private static final ThreadLocal<BufferClaim> bufferClaims = ThreadLocal.withInitial(BufferClaim::new);

    private static final ThreadLocal<UnsafeBuffer> unsafeBuffers = ThreadLocal.withInitial(() -> new UnsafeBuffer(Constants.EMTPY));

    void offer(Publication publication, ByteBuffer byteBuffer, int length) {
        final byte[] bytes = new byte[length];
        final UnsafeBuffer unsafeBuffer = unsafeBuffers.get();
        unsafeBuffer.wrap(bytes);

        unsafeBuffer.putShort(0, (short) 0);
        unsafeBuffer.putShort(BitUtil.SIZE_OF_SHORT, (short) MessageType.FRAME.getEncodedType());
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
                    buffer.putShort(offset, (short) 0);
                    buffer.putShort(offset + BitUtil.SIZE_OF_SHORT, (short) MessageType.FRAME.getEncodedType());
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

                System.out.println(Thread.currentThread() + " - Sending establishConnection message");
                publication.offer(buffer);
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

    public static boolean isRunning() {
        return running;
    }

    public static void setRunning(boolean running) {
        ReactivesocketAeronClient.running = running;
    }

    public int getSessionId() {
        return sessionId;
    }

    public void setSessionId(int sessionId) {
        this.sessionId = sessionId;
    }

    public int getPort() {
        return port;
    }

    public int getServerSessionId() {
        return serverSessionId;
    }

    public void setServerSessionId(int serverSessionId) {
        this.serverSessionId = serverSessionId;
    }

    public static boolean isPollingStarted() {
        return pollingStarted;
    }

    public static void setPollingStarted(boolean pollingStarted) {
        ReactivesocketAeronClient.pollingStarted = pollingStarted;
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
        closeQuietly(reactiveSockets.get(sessionId));
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
