package io.reactivesocket.aeron;

import io.reactivesocket.Completable;
import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.internal.EmptyDisposable;
import io.reactivesocket.observable.Observable;
import io.reactivesocket.observable.Observer;
import org.reactivestreams.Publisher;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.agrona.MutableDirectBuffer;

import java.util.concurrent.TimeUnit;

public class AeronServerDuplexConnection implements DuplexConnection, AutoCloseable, Loggable {

    private static final ThreadLocal<BufferClaim> bufferClaims = ThreadLocal.withInitial(BufferClaim::new);

    private Publication publication;
    private Observer<Frame> observer;
    private Observable<Frame> observable;

    public AeronServerDuplexConnection(
        Publication publication) {
        this.publication = publication;
        this.observable = new Observable<Frame>() {
            @Override
            public void subscribe(Observer<Frame> o) {
                observer = o;
                o.onSubscribe(new EmptyDisposable());
            }
        };
    }

    public Observer<Frame> getSubscriber() {
        return observer;
    }

    public Observable<Frame> getInput() {
        return observable;
    }

    @Override
    public void addOutput(Publisher<Frame> o, Completable callback) {
        o.subscribe(new CompletableSubscription(publication, callback, this));
    }

    void ackEstablishConnection(int ackSessionId) {
        final long start = System.nanoTime();
        final int sessionId = publication.sessionId();
        final BufferClaim bufferClaim = bufferClaims.get();

        debug("Acking establish connection for session id => {}",  ackSessionId);

        for (;;) {
            final long current = System.nanoTime();
            if ((current - start) > TimeUnit.SECONDS.toNanos(30)) {
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
        observer.onComplete();
        publication.close();
    }
}
