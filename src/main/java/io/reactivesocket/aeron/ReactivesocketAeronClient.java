package io.reactivesocket.aeron;

import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.Frame;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import rx.Scheduler;
import rx.schedulers.Schedulers;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.FragmentAssembler;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.reactivesocket.aeron.Constants.CLIENT_STREAM_ID;
import static io.reactivesocket.aeron.Constants.EMTPY;
import static io.reactivesocket.aeron.Constants.SERVER_STREAM_ID;

/**
 * Created by rroeser on 8/13/15.
 */
public class ReactivesocketAeronClient implements Loggable {
    private static final ConcurrentHashMap<Integer, Subscription> subscriptions = new ConcurrentHashMap<>();

    private static final ConcurrentHashMap<Integer, AeronClientDuplexConnection> connections = new ConcurrentHashMap<>();

    private static final ConcurrentHashMap<Integer, CountDownLatch> establishConnectionLatches = new ConcurrentHashMap<>();

    private ReactiveSocket reactiveSocket;

    private final Aeron aeron;

    private final Publication publication;

    private volatile static boolean running = true;

    private static final CountDownLatch shutdownLatch = new CountDownLatch(1);

    private final int port;

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
    }

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
        publication = aeron.addPublication(channel, SERVER_STREAM_ID);
        final int sessionId = publication.sessionId();

        System.out.println("Created a publication for sessionId => " + sessionId);

        subscriptions.computeIfAbsent(port, (_p) -> {
            System.out.println("Creating a subscription to channel => " + subscriptionChannel);
            Subscription subscription = aeron.addSubscription(subscriptionChannel, CLIENT_STREAM_ID);
            System.out.println("Subscription created to channel => " + subscriptionChannel);

            final FragmentAssembler fragmentAssembler = new FragmentAssembler(this::fragmentHandler);

            poll(fragmentAssembler, subscription, Schedulers.computation().createWorker());

            return subscription;
        });

        establishConnection(publication, sessionId);

    }

    public static ReactivesocketAeronClient create(String host, String server, int port) {
        return new ReactivesocketAeronClient(host, server, port);
    }

    public static ReactivesocketAeronClient create(String host, String server) {
        return new ReactivesocketAeronClient(host, server, 39790);
    }

    void fragmentHandler(DirectBuffer buffer, int offset, int length, Header header) {
        int messageTypeInt = buffer.getInt(offset);
        MessageType messageType = MessageType.from(messageTypeInt);
        if (messageType == MessageType.FRAME) {
            final AeronClientDuplexConnection connection = connections.get(header.sessionId());
            final Subscriber<? super Frame> subscriber = connection.getSubscriber();
            final ByteBuffer bytes = ByteBuffer.allocate(length);
            buffer.getBytes(BitUtil.SIZE_OF_INT + offset, bytes, length);
            final Frame frame = Frame.from(bytes);
            subscriber.onNext(frame);
        } else if (messageType == MessageType.ESTABLISH_CONNECTION_RESPONSE) {
            int ackSessionId = buffer.getInt(offset + BitUtil.SIZE_OF_INT);
            System.out.println(String.format("Received establish connection ack for session id => %d", ackSessionId));
            final int headerSessionId = header.sessionId();
            final AeronClientDuplexConnection connection =
                connections
                    .computeIfAbsent(headerSessionId, (_p) ->
                        new AeronClientDuplexConnection(publication, () -> connections.remove(headerSessionId)));

            reactiveSocket =  ReactiveSocket.fromClientConnection(
                connection,
                ConnectionSetupPayload.create("UTF-8", "UTF-8", ConnectionSetupPayload.NO_FLAGS),
                err -> err.printStackTrace());

            reactiveSocket.start();

            System.out.println("ReactiveSocket connected to Aeron session => " + ackSessionId);
            CountDownLatch latch = establishConnectionLatches.get(ackSessionId);
            latch.countDown();

        } else {
            System.out.println("Unknown message type => " + messageTypeInt);
        }
    }

    void poll(FragmentAssembler fragmentAssembler, Subscription subscription, Scheduler.Worker worker) {
        worker.schedule(() -> {
            if (running) {
                try {
                    subscription.poll(fragmentAssembler, Integer.MAX_VALUE);
                    poll(fragmentAssembler, subscription, worker);
                } catch (Throwable t) {
                    error(t.getMessage(), t);
                }
            } else {
                shutdownLatch.countDown();
            }
        });
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

}
