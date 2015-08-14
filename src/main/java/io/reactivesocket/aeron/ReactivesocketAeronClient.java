package io.reactivesocket.aeron;

import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.ReactiveSocketClientProtocol;
import org.reactivestreams.Publisher;
import rx.Observable;
import rx.RxReactiveStreams;
import rx.Scheduler;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;
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
public class ReactivesocketAeronClient {
    private static final ThreadLocal<UnsafeBuffer> buffers = ThreadLocal.withInitial(() -> new UnsafeBuffer(EMTPY));

    private static final Int2ObjectHashMap<Subscription> subscriptions = new Int2ObjectHashMap<>();

    private static final Int2ObjectHashMap<PublishSubject<Frame>> subjects = new Int2ObjectHashMap<>();

    private static final Int2ObjectHashMap<CountDownLatch> establishConnectionLatches = new Int2ObjectHashMap<>();

    private final ReactiveSocketClientProtocol rsClientProtocol;

    private final Aeron aeron;

    private volatile boolean running = true;

    private final int port;

    private ReactivesocketAeronClient(String host, int port) {
        this.port = port;

        final Aeron.Context ctx = new Aeron.Context();
        aeron = Aeron.connect(ctx);

        final String channel = "udp://" + host + ":" + port;

        System.out.println("Creating a publication to channel => " + channel);

        final Publication publication = aeron.addPublication(channel, SERVER_STREAM_ID);

        final int sessionId = publication.sessionId();

        subjects.computeIfAbsent(sessionId, (_p) -> PublishSubject.create());

        subscriptions.computeIfAbsent(port, (_p) -> {
            Subscription subscription = aeron.addSubscription(channel, CLIENT_STREAM_ID);

            final FragmentAssembler fragmentAssembler = new FragmentAssembler(this::fragmentHandler);

            poll(fragmentAssembler, subscription, Schedulers.computation().createWorker());

            return subscription;
        });

        establishConnection(publication, sessionId);

        this.rsClientProtocol =
            ReactiveSocketClientProtocol.create(new DuplexConnection() {

                public Publisher<Frame> getInput() {
                    PublishSubject publishSubject = subjects.get(sessionId);
                    return RxReactiveStreams.toPublisher(publishSubject);
                }

                @Override
                public Publisher<Void> write(Publisher<Frame> o) {
                    Observable<Void> req = RxReactiveStreams
                        .toObservable(o)
                        .map(frame -> {
                            final ByteBuffer frameBuffer = frame.getByteBuffer();
                            final int frameBufferLength = frameBuffer.capacity();
                            final UnsafeBuffer buffer = buffers.get();
                            final byte[] bytes = new byte[frameBufferLength + BitUtil.SIZE_OF_INT];

                            buffer.wrap(bytes);
                            buffer.putInt(0, MessageType.FRAME.getEncodedType());
                            buffer.putBytes(BitUtil.SIZE_OF_INT, frameBuffer, frameBufferLength);

                            for (;;) {
                                final long offer = publication.offer(buffer);

                                if (offer >= 0) {
                                    break;
                                } else if (Publication.NOT_CONNECTED == offer) {
                                    throw new RuntimeException("not connected");
                                }
                            }

                            return null;
                        });

                    return RxReactiveStreams.toPublisher(req);
                }
            });
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
            final PublishSubject<Frame> subject = subjects.get(header.sessionId());
            ByteBuffer bytes = ByteBuffer.allocate(buffer.capacity());
            buffer.getBytes(BitUtil.SIZE_OF_INT, bytes, buffer.capacity());
            final Frame frame = Frame.from(bytes);
            subject.onNext(frame);
        } else if (messageType == MessageType.ESTABLISH_CONNECTION_RESPONSE) {
            int ackSessionId = buffer.getInt(offset + BitUtil.SIZE_OF_INT);
            System.out.println(String.format("Received establish connection ack for session id => %d", ackSessionId));
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
     * Establishes a connection between the client and server. Waits for 39 seconds before throwing a exception.
     */
    void establishConnection(final Publication publication, final int sessionId) {
        try {
            UnsafeBuffer buffer = buffers.get();
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

    public Publisher<String> requestResponse(String payload) {
        return rsClientProtocol.requestResponse(payload);
    }

    public Publisher<String> requestStream(String payload) {
        return rsClientProtocol.requestStream(payload);
    }

    public Publisher<Void> fireAndForget(String payload) {
        return rsClientProtocol.fireAndForget(payload);
    }

    public Publisher<String> requestSubscription(String payload) {
        return rsClientProtocol.requestSubscription(payload);
    }

}
