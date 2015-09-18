package io.reactivesocket.aeron.server;

import io.reactivesocket.Completable;
import io.reactivesocket.Frame;
import io.reactivesocket.aeron.internal.Constants;
import io.reactivesocket.aeron.internal.MessageType;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by rroeser on 8/27/15.
 */
public class CompletableSubscription implements Subscriber<Frame> {

    private static final ThreadLocal<BufferClaim> bufferClaims = ThreadLocal.withInitial(BufferClaim::new);

    private static final ThreadLocal<UnsafeBuffer> unsafeBuffers = ThreadLocal.withInitial(() -> new UnsafeBuffer(Constants.EMTPY));

    private final Publication publication;

    private final Completable completable;

    private final AutoCloseable closeable;

    private final int mtuLength;

    private volatile static AtomicLong count = new AtomicLong();

    public CompletableSubscription(Publication publication, Completable completable, AutoCloseable closeable) {
        this.publication = publication;
        this.completable = completable;
        this.closeable = closeable;

        String mtuLength = System.getProperty("aeron.mtu.length", "4096");

        this.mtuLength = Integer.parseInt(mtuLength);
    }

    @Override
    public void onSubscribe(Subscription s) {
        s.request(1);
    }

    @Override
    public void onNext(Frame frame) {
        final ByteBuffer byteBuffer = frame.getByteBuffer();
        final int length = byteBuffer.capacity() + BitUtil.SIZE_OF_INT;

        // If the length is less the MTU size send the message using tryClaim which does not fragment the message
        // If the message is larger the the MTU size send it using offer.
        if (length < mtuLength) {
            tryClaim(byteBuffer, length);
        } else {
            offer(byteBuffer, length);
        }
    }

    @Override
    public void onError(Throwable t) {
        completable.error(t);
    }

    @Override
    public void onComplete() {
        completable.success();
    }

    private short getCount() {
        return (short) count.incrementAndGet();
    }

    void offer(ByteBuffer byteBuffer, int length) {
        final byte[] bytes = new byte[length];
        final UnsafeBuffer unsafeBuffer = unsafeBuffers.get();
        unsafeBuffer.wrap(bytes);
        unsafeBuffer.putShort(0, getCount());
        unsafeBuffer.putShort(BitUtil.SIZE_OF_SHORT, (short)  MessageType.FRAME.getEncodedType());
        unsafeBuffer.putBytes(BitUtil.SIZE_OF_INT, byteBuffer, byteBuffer.capacity());
        do {
            final long offer = publication.offer(unsafeBuffer);
            if (offer >= 0) {
                break;
            } else if (Publication.NOT_CONNECTED == offer) {
                closeQuietly(closeable);
                completable.error(new RuntimeException("not connected"));
                break;
            }
        } while(true);

    }

    void tryClaim(ByteBuffer byteBuffer, int length) {
        final BufferClaim bufferClaim = bufferClaims.get();
        do {
            final long offer = publication.tryClaim(length, bufferClaim);
            if (offer >= 0) {
                try {
                    final MutableDirectBuffer buffer = bufferClaim.buffer();
                    final int offset = bufferClaim.offset();
                    //System.out.println("server count => " + count);
                    buffer.putShort(offset, getCount());
                    buffer.putShort(offset + BitUtil.SIZE_OF_SHORT, (short) MessageType.FRAME.getEncodedType());
                    buffer.putBytes(offset + BitUtil.SIZE_OF_INT, byteBuffer, 0, byteBuffer.capacity());
                } finally {
                    bufferClaim.commit();
                }

                break;
            } else if (Publication.NOT_CONNECTED == offer) {
                closeQuietly(closeable);
                completable.error(new RuntimeException("not connected"));
                break;
            }
        } while(true);
    }

    void closeQuietly(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception e) {
            // ignore
        }
    }
}
