package io.reactivesocket.aeron;

import io.reactivesocket.Frame;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

public class PublishSubscription implements Subscription {
    private final Subscriber<? super Void> subscriber;

    private final Publisher<Frame> source;

    private static final ThreadLocal<BufferClaim> bufferClaims = ThreadLocal.withInitial(BufferClaim::new);

    private static final ThreadLocal<UnsafeBuffer> unsafeBuffers = ThreadLocal.withInitial(() -> new UnsafeBuffer(Constants.EMTPY));

    private Publication publication;

    private final int mtuLength;


    public PublishSubscription(Subscriber<? super Void> subscriber, Publisher<Frame> source, Publication publication) {
        this.source = source;
        this.subscriber = subscriber;
        this.publication = publication;

        String mtuLength = System.getProperty("aeron.mtu.length", "4096");

        this.mtuLength = Integer.parseInt(mtuLength);

        request(1);
    }

    @Override
    public void cancel() {

    }

    @Override
    public void request(long n) {
        source.subscribe(new Subscriber<Frame>() {
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
                subscriber.onError(t);
            }

            @Override
            public void onComplete() {
                subscriber.onComplete();
            }
        });
    }

    void offer(ByteBuffer byteBuffer, int length) {
        final byte[] bytes = new byte[length];
        final UnsafeBuffer unsafeBuffer = unsafeBuffers.get();
        unsafeBuffer.wrap(bytes);
        unsafeBuffer.putInt(0, MessageType.FRAME.getEncodedType());
        unsafeBuffer.putBytes(BitUtil.SIZE_OF_INT, byteBuffer, byteBuffer.capacity());
        for (;;) {
            final long offer = publication.offer(unsafeBuffer);
            if (offer >= 0) {
                break;
            } else if (Publication.NOT_CONNECTED == offer) {
                subscriber.onError(new RuntimeException("not connected"));
                break;
            }
        }

    }

    void tryClaim(ByteBuffer byteBuffer, int length) {
        final BufferClaim bufferClaim = bufferClaims.get();
        for (;;) {
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
                subscriber.onError(new RuntimeException("not connected"));
                break;
            }
        }
    }
}
