package io.reactivesocket.aeron;

import io.reactivesocket.Frame;
import io.reactivesocket.ReactiveSocketServerProtocol;
import io.reactivesocket.RequestHandler;
import rx.Scheduler;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.FragmentAssembler;
import uk.co.real_logic.aeron.Image;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.collections.Int2ObjectHashMap;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ReactiveSocketAeronServer implements Closeable {

    private final ReactiveSocketServerProtocol rsServerProtocol;

    private final Aeron aeron;

    private final int SERVER_STREAM_ID = 1;

    private final int CLIENT_STREAM_ID = 2;

    private final int port;

    private final Int2ObjectHashMap<AeronServerDuplexConnection> connections;

    private final Scheduler.Worker worker;

    private final Subscription subscription;

    private volatile boolean running = true;

    private ReactiveSocketAeronServer(int port, RequestHandler requestHandler) {
        this.port = port;
        this.connections = new Int2ObjectHashMap<>();

        final Aeron.Context ctx = new Aeron.Context();
        ctx.newImageHandler(this::newImageHandler);

        aeron = Aeron.connect(ctx);

        subscription = aeron.addSubscription("udp://localhost:" + port, SERVER_STREAM_ID);

        final FragmentAssembler fragmentAssembler = new FragmentAssembler(this::fragmentHandler);

        worker = Schedulers.computation().createWorker();

        poll(fragmentAssembler);

        rsServerProtocol = ReactiveSocketServerProtocol.create(requestHandler);
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
        AeronServerDuplexConnection connection = connections.get(sessionId);

        if (connection != null) {
            final PublishSubject<Frame> subject = connection.getSubject();
            ByteBuffer bytes = ByteBuffer.allocate(buffer.capacity());
            buffer.getBytes(0, bytes, buffer.capacity());
            final Frame frame = Frame.from(bytes);
            subject.onNext(frame);
        } else {
            System.out.println("No connection found for session id " + sessionId);
        }
    }

    void newImageHandler(Image image, String channel, int streamId, int sessionId, long joiningPosition, String sourceIdentity) {
        if (SERVER_STREAM_ID == streamId) {

            final AeronServerDuplexConnection connection = connections.computeIfAbsent(sessionId, (_s) -> {
                final String responseChannel = "udp://" + sourceIdentity.substring(0, sourceIdentity.indexOf(':')) + ":" + port;
                Publication publication = aeron.addPublication(responseChannel, CLIENT_STREAM_ID);
                return new AeronServerDuplexConnection(publication, streamId, sessionId);
            });
            rsServerProtocol.acceptConnection(connection);
        } else {
            System.out.println("Unsupported stream id " + streamId);
        }
    }

    @Override
    public void close() throws IOException {
        running = false;
        worker.unsubscribe();
        aeron.close();
    }
}
