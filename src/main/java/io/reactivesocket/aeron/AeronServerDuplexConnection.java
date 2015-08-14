package io.reactivesocket.aeron;

import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import org.reactivestreams.Publisher;
import rx.Observable;
import rx.RxReactiveStreams;
import rx.subjects.PublishSubject;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.agrona.MutableDirectBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

public class AeronServerDuplexConnection implements DuplexConnection, AutoCloseable {

    private static final ThreadLocal<BufferClaim> bufferClaims = ThreadLocal.withInitial(BufferClaim::new);

    private Publication publication;
    private PublishSubject<Frame> subject;

    public AeronServerDuplexConnection(
        Publication publication) {
        this.publication = publication;
        this.subject = PublishSubject.create();
    }

    PublishSubject<Frame> getSubject() {
        return subject;
    }

    @Override
    public Publisher<Frame> getInput() {
        return RxReactiveStreams.toPublisher(subject);
    }

    public Publisher<Void> write(Publisher<Frame> o) {
        Observable<Void> req = RxReactiveStreams
            .toObservable(o)
            .map(frame -> {
                final ByteBuffer byteBuffer = frame.getByteBuffer();
                final int length = byteBuffer.capacity() + BitUtil.SIZE_OF_INT;
                for (;;) {
                    final BufferClaim bufferClaim = bufferClaims.get();
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

                }

                return null;
            });

        return RxReactiveStreams.toPublisher(req);
    }

    void ackEstablishConnection(int ackSessionId) {
        final long start = System.nanoTime();
        final int sessionId = publication.sessionId();
        final BufferClaim bufferClaim = bufferClaims.get();

        System.out.print("Acking establish connection for session id => " + ackSessionId);

        for (;;) {
            final long current = System.nanoTime();
            if (current - start > TimeUnit.SECONDS.toNanos(30)) {
                throw new RuntimeException("Timed out waiting to establish connection for session id => " + sessionId);
            }

            final long offer = publication.tryClaim(2 * BitUtil.SIZE_OF_INT, bufferClaim);
            if (offer >= 0) {
                try {
                    final MutableDirectBuffer buffer = bufferClaim.buffer();
                    final int offeset = bufferClaim.offset();
                    buffer.putInt(offeset, MessageType.ESTABLISH_CONNECTION_RESPONSE.getEncodedType());
                    buffer.putInt(offeset + BitUtil.SIZE_OF_INT, ackSessionId);
                } finally {
                    bufferClaim.commit();
                }

                break;
            }

        }
    }

    @Override
    public void close() throws Exception {
        subject.onCompleted();
        publication.close();
    }
}
