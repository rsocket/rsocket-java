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
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.collections.Int2ObjectHashMap;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

/**
 * Created by rroeser on 8/13/15.
 */
public class ReactivesocketAeronClient {
    private static final byte[] EMTPY = new byte[0];

    private static final ThreadLocal<UnsafeBuffer> buffers = ThreadLocal.withInitial(() -> new UnsafeBuffer(EMTPY));

    private static final Int2ObjectHashMap<Subscription> subscriptions = new Int2ObjectHashMap<>();

    private static final Int2ObjectHashMap<PublishSubject<Frame>> subjects = new Int2ObjectHashMap<>();

    private static final int SERVER_STREAM_ID = 1;

    private static final int CLIENT_STREAM_ID = 2;

    private final ReactiveSocketClientProtocol rsClientProtocol;

    private final Aeron aeron;

    private volatile boolean running = true;

    private final int port;

    private ReactivesocketAeronClient(String host, int port) {
        this.port = port;

        final Aeron.Context ctx = new Aeron.Context();
        aeron = Aeron.connect(ctx);

        final String channel = "udp://" + host + ":" + port;

        final Publication publication = aeron.addPublication(channel, SERVER_STREAM_ID);

        final int sessionId = publication.sessionId();

        subjects.computeIfAbsent(sessionId, (_p) -> PublishSubject.create());

        subscriptions.computeIfAbsent(port, (_p) -> {
            Subscription subscription = aeron.addSubscription(channel, CLIENT_STREAM_ID);

            final FragmentAssembler fragmentAssembler = new FragmentAssembler(this::fragmentHandler);

            poll(fragmentAssembler, subscription, Schedulers.computation().createWorker());

            return subscription;
        });

        this.rsClientProtocol =
            ReactiveSocketClientProtocol.create(new DuplexConnection() {

                public Publisher<Frame> getInput() {
                    PublishSubject publishSubject = subjects.get(port);
                    return RxReactiveStreams.toPublisher(publishSubject);
                }

                @Override
                public Publisher<Void> write(Publisher<Frame> o) {
                    Observable<Void> req = RxReactiveStreams
                        .toObservable(o)
                        .map(frame -> {
                            final UnsafeBuffer buffer = buffers.get();
                            ByteBuffer byteBuffer = frame.getByteBuffer();
                            buffer.wrap(byteBuffer);

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
        final PublishSubject<Frame> subject = subjects.get(header.sessionId());
        ByteBuffer bytes = ByteBuffer.allocate(buffer.capacity());
        buffer.getBytes(0, bytes, buffer.capacity());
        final Frame frame = Frame.from(bytes);
        subject.onNext(frame);
    }

    void poll(FragmentAssembler fragmentAssembler, Subscription subscription, Scheduler.Worker worker) {
        if (running) {
            worker.schedule(() -> {
                subscription.poll(fragmentAssembler, Integer.MAX_VALUE);
                poll(fragmentAssembler, subscription, worker);
            });
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
