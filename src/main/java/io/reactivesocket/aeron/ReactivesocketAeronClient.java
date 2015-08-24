package io.reactivesocket.aeron;

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
import uk.co.real_logic.agrona.collections.Int2ObjectHashMap;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.reactivesocket.aeron.Constants.CLIENT_STREAM_ID;
import static io.reactivesocket.aeron.Constants.EMTPY;
import static io.reactivesocket.aeron.Constants.SERVER_STREAM_ID;

/**
 * Created by rroeser on 8/13/15.
 */
public class ReactivesocketAeronClient implements AutoCloseable {
    private static final Int2ObjectHashMap<Subscription> subscriptions = new Int2ObjectHashMap<>();

    private static final Int2ObjectHashMap<AeronClientDuplexConnection> connections = new Int2ObjectHashMap<>();

    private static final Int2ObjectHashMap<CountDownLatch> establishConnectionLatches = new Int2ObjectHashMap<>();

    private final ReactiveSocket rsClientProtocol;

    private final Aeron aeron;

    private final Publication publication;

    private volatile boolean running = true;

    private final int port;

    private ReactivesocketAeronClient(String host, int port) {
        this.port = port;
        this.rsClientProtocol =
            ReactiveSocket.createRequestor();

        final Aeron.Context ctx = new Aeron.Context();
        aeron = Aeron.connect(ctx);

        final String channel = "udp://" + host + ":" + port;
        System.out.println("Creating a publication to channel => " + channel);

        publication = aeron.addPublication(channel, SERVER_STREAM_ID);
        final int sessionId = publication.sessionId();
        subscriptions.computeIfAbsent(port, (_p) -> {
            Subscription subscription = aeron.addSubscription(channel, CLIENT_STREAM_ID);

            final FragmentAssembler fragmentAssembler = new FragmentAssembler(this::fragmentHandler);

            poll(fragmentAssembler, subscription, Schedulers.computation().createWorker());

            return subscription;
        });

        establishConnection(publication, sessionId);

    }

    public static ReactivesocketAeronClient create(String host, int port) {
        return new ReactivesocketAeronClient(host, port);
    }

    public static ReactivesocketAeronClient create(String host) {
        return new ReactivesocketAeronClient(host, 39790);
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

            final AeronClientDuplexConnection connection = connections.computeIfAbsent(header.sessionId(), (_p) -> new AeronClientDuplexConnection(publication));

            Publisher<Void> connect = this
                .rsClientProtocol
                .connect(connection);

            connect.subscribe(new Subscriber<Void>() {
                @Override
                public void onSubscribe(org.reactivestreams.Subscription s) {
                    s.request(Long.MAX_VALUE);
                }

                @Override
                public void onNext(Void aVoid) {

                }

                @Override
                public void onError(Throwable t) {
                    t.printStackTrace();
                }

                @Override
                public void onComplete() {
                }
            });

            System.out.println("ReactiveSocket connected to Aeron session => " + ackSessionId);
            CountDownLatch latch = establishConnectionLatches.get(ackSessionId);
            latch.countDown();

        } else {
            System.out.println("Unknow message type => " + messageTypeInt);
        }
    }

    void poll(FragmentAssembler fragmentAssembler, Subscription subscription, Scheduler.Worker worker) {
        if (running) {
            worker.schedule(() -> {
                subscription.poll(fragmentAssembler, Integer.MAX_VALUE);
                poll(fragmentAssembler, subscription, worker);
            });
        }
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
                if (current - start > TimeUnit.SECONDS.toNanos(30)) {
                    throw new RuntimeException("Timed out waiting to establish connection for session id => " + sessionId);
                }

                if (offer < 0) {
                    offer = publication.offer(buffer);
                }

                if (latch.getCount() == 0) {
                    break;
                }
            }

            System.out.println(String.format("Connection established for channel => %s, stream id => %d",
                publication.channel(),
                publication.sessionId()));
        } finally {
            establishConnectionLatches.remove(sessionId);
        }

    }

    public Publisher<Payload> requestResponse(Payload payload) {
        return rsClientProtocol.requestResponse(payload);
    }

    public Publisher<Void> fireAndForget(Payload payload) {
        return rsClientProtocol.fireAndForget(payload);
    }

    public Publisher<Payload> requestStream(Payload payload) {
        return rsClientProtocol.requestStream(payload);
    }

    public Publisher<Payload> requestSubscription(Payload payload) {
        return rsClientProtocol.requestSubscription(payload);
    }

    @Override
    public void close() throws Exception {
        for (Subscription subscription : subscriptions.values()) {
            subscription.close();
        }

        for (AeronClientDuplexConnection connection : connections.values()) {
            connection.close();
        }
    }
}
