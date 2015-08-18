package io.reactivesocket.aeron;

import io.reactivesocket.Frame;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.RequestHandler;
import org.reactivestreams.Subscriber;
import rx.Scheduler;
import rx.schedulers.Schedulers;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.FragmentAssembler;
import uk.co.real_logic.aeron.Image;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.collections.Int2ObjectHashMap;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static io.reactivesocket.aeron.Constants.CLIENT_STREAM_ID;
import static io.reactivesocket.aeron.Constants.SERVER_STREAM_ID;

public class ReactiveSocketAeronServer implements AutoCloseable {

    private final Aeron aeron;

    private final int port;

    private volatile Int2ObjectHashMap<AeronServerDuplexConnection> connections;

    private final Scheduler.Worker worker;

    private final Subscription subscription;

    private volatile boolean running = true;

    private final RequestHandler requestHandler;

    private static final org.reactivestreams.Subscriber<Void> PROTOCOL_SUBSCRIBER = new org.reactivestreams.Subscriber<Void>() {
        @Override
        public void onSubscribe(org.reactivestreams.Subscription s) {
            s.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(Void t) {

        }

        @Override
        public void onError(Throwable t) {
            t.printStackTrace();
        }

        @Override
        public void onComplete() {

        }
    };

    private ReactiveSocketAeronServer(int port, RequestHandler requestHandler) {
        this.port = port;
        this.connections = new Int2ObjectHashMap<>();
        this.requestHandler = requestHandler;

        final Aeron.Context ctx = new Aeron.Context();
        ctx.newImageHandler(this::newImageHandler);

        aeron = Aeron.connect(ctx);

        subscription = aeron.addSubscription("udp://localhost:" + port, SERVER_STREAM_ID);

        final FragmentAssembler fragmentAssembler = new FragmentAssembler(this::fragmentHandler);

        worker = Schedulers.computation().createWorker();

        poll(fragmentAssembler);

    }

    public static ReactiveSocketAeronServer create(int port, RequestHandler requestHandler) {
        return new ReactiveSocketAeronServer(port, requestHandler);
    }

    public static ReactiveSocketAeronServer create(RequestHandler requestHandler) {
        return new ReactiveSocketAeronServer(39790, requestHandler);
    }

    void poll(FragmentAssembler fragmentAssembler) {
        if (running) {
            worker.schedule(() -> {
                subscription.poll(fragmentAssembler, Integer.MAX_VALUE);
                poll(fragmentAssembler);
            });
        }
    }

    void fragmentHandler(DirectBuffer buffer, int offset, int length, Header header) {
        final int sessionId = header.sessionId();

        int messageTypeInt = buffer.getInt(offset);
        MessageType type = MessageType.from(messageTypeInt);

        if (MessageType.FRAME == type) {
            AeronServerDuplexConnection connection = connections.get(sessionId);
            if (connection != null) {
                final Subscriber<? super Frame> subscriber = connection.getSubscriber();
                ByteBuffer bytes = ByteBuffer.allocate(length);
                buffer.getBytes(BitUtil.SIZE_OF_INT + offset, bytes, length);
                final Frame frame = Frame.from(bytes);
                subscriber.onNext(frame);
            }
        } else if (MessageType.ESTABLISH_CONNECTION_REQUEST == type) {
            final long start = System.nanoTime();
            AeronServerDuplexConnection connection = null;
            System.out.println("Looking a connection to ack establish connection for session id => " + sessionId);
            while (connection == null) {
                final long current = System.nanoTime();

                if (current - start > TimeUnit.SECONDS.toNanos(30)) {
                    throw new RuntimeException("unable to find connection to ack establish connection for session id => " + sessionId);
                }

                connection = connections.get(sessionId);
            }
            System.out.println("Found a connection to ack establish connection for session id => " + sessionId);
            connection.ackEstablishConnection(sessionId);
        }

    }

    void newImageHandler(Image image, String channel, int streamId, int sessionId, long joiningPosition, String sourceIdentity) {
        if (SERVER_STREAM_ID == streamId) {
            System.out.println(String.format("Handling new image for session id => %d and stream id => %d", streamId, sessionId));
            final AeronServerDuplexConnection connection = connections.computeIfAbsent(sessionId, (_s) -> {
                final String responseChannel = "udp://" + sourceIdentity.substring(0, sourceIdentity.indexOf(':')) + ":" + port;
                Publication publication = aeron.addPublication(responseChannel, CLIENT_STREAM_ID);
                System.out.println(String.format("Creating new connection for responseChannel => %s, streamId => %d, and sessionId => %d", responseChannel, streamId, sessionId));
                return new AeronServerDuplexConnection(publication);
            });
            System.out.println("Accepting ReactiveSocket connection");
            ReactiveSocket socket = ReactiveSocket.accept(connection, requestHandler);

            socket.responderPublisher().subscribe(PROTOCOL_SUBSCRIBER);
        } else {
            System.out.println("Unsupported stream id " + streamId);
        }
    }

    @Override
    public void close() throws Exception {
        running = false;
        worker.unsubscribe();
        aeron.close();

        for (AeronServerDuplexConnection connection : connections.values()) {
            connection.close();
        }
    }

}
