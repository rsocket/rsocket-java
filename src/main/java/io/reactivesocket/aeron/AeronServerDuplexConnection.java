package io.reactivesocket.aeron;

import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import org.reactivestreams.Publisher;
import rx.Observable;
import rx.RxReactiveStreams;
import rx.subjects.PublishSubject;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.agrona.MutableDirectBuffer;

import java.nio.ByteBuffer;

public class AeronServerDuplexConnection implements DuplexConnection {
    private static final byte[] EMTPY = new byte[0];

    private static final ThreadLocal<BufferClaim> bufferClaims = ThreadLocal.withInitial(BufferClaim::new);

    private Publication publication;
    private PublishSubject<Frame> subject;

    private int aeronStreamId;
    private int aeronSessionId;

    public AeronServerDuplexConnection(
        Publication publication,
        int aeronStreamId,
        int aeronSessionId) {
        this.publication = publication;
        this.subject = PublishSubject.create();
        this.aeronStreamId = aeronStreamId;
        this.aeronSessionId = aeronSessionId;
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

                for (;;) {
                    final BufferClaim bufferClaim = bufferClaims.get();
                    final long offer = publication.tryClaim(byteBuffer.capacity(), bufferClaim);
                    if (offer >= 0) {
                        try {
                            final MutableDirectBuffer buffer = bufferClaim.buffer();
                            final int offset = bufferClaim.offset();
                            buffer.putBytes(offset, byteBuffer, 0, byteBuffer.capacity());
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
}
