package io.reactivesocket.aeron;

import io.reactivesocket.Completable;
import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.agrona.MutableDirectBuffer;

import java.util.concurrent.TimeUnit;

public class AeronServerDuplexConnection implements DuplexConnection, AutoCloseable {

    private static final ThreadLocal<BufferClaim> bufferClaims = ThreadLocal.withInitial(BufferClaim::new);

    private Publication publication;
    private Subscriber<? super Frame> subscriber;
    private Publisher<Frame> publisher;

    public AeronServerDuplexConnection(
        Publication publication) {
        this.publication = publication;
        this.publisher = (Subscriber<? super Frame> s) -> subscriber = s;
    }

    public Subscriber<? super Frame> getSubscriber() {
        return subscriber;
    }

    @Override
    public Publisher<Frame> getInput() {
        return publisher;
    }

    @Override
    public void addOutput(Publisher<Frame> o, Completable callback) {
        o.subscribe(new CompletableSubscription(publication, callback));
    }

    void ackEstablishConnection(int ackSessionId) {
        final long start = System.nanoTime();
        final int sessionId = publication.sessionId();
        final BufferClaim bufferClaim = bufferClaims.get();

        System.out.println("Acking establish connection for session id => " + ackSessionId);

        for (;;) {
            final long current = System.nanoTime();
            if (current - start > TimeUnit.SECONDS.toNanos(30)) {
                throw new RuntimeException("Timed out waiting to establish connection for session id => " + sessionId);
            }

            final long offer = publication.tryClaim(2 * BitUtil.SIZE_OF_INT, bufferClaim);
            if (offer >= 0) {
                try {
                    final MutableDirectBuffer buffer = bufferClaim.buffer();
                    final int offset = bufferClaim.offset();
                    buffer.putInt(offset, MessageType.ESTABLISH_CONNECTION_RESPONSE.getEncodedType());
                    buffer.putInt(offset + BitUtil.SIZE_OF_INT, ackSessionId);
                } finally {
                    bufferClaim.commit();
                }

                break;
            }

        }
    }

    @Override
    public void close() {
        subscriber.onComplete();
        publication.close();
    }
}
