package io.reactivesocket.aeron;

import io.reactivesocket.Frame;
import rx.Observable;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

class OperatorPublish implements  Observable.Operator<Void, Frame> {

    private static final ThreadLocal<BufferClaim> bufferClaims = ThreadLocal.withInitial(BufferClaim::new);

    private static final ThreadLocal<UnsafeBuffer> unsafeBuffers = ThreadLocal.withInitial(() -> new UnsafeBuffer(Constants.EMTPY));

    private Publication publication;

    public OperatorPublish(Publication publication) {
        this.publication = publication;
    }

    @Override
    public rx.Subscriber<? super Frame> call(rx.Subscriber<? super Void> child) {
        return new rx.Subscriber<Frame>(child) {
            @Override
            public void onStart() {
                request(1);
            }

            @Override
            public void onCompleted() {
                child.onCompleted();
            }

            @Override
            public void onError(Throwable e) {
                child.onError(e);
            }

            @Override
            public void onNext(Frame frame) {

                final ByteBuffer byteBuffer = frame.getByteBuffer();
                final int length = byteBuffer.capacity() + BitUtil.SIZE_OF_INT;

                // If the length is less the MTU size send the message using tryClaim which does not fragment the message
                // If the message is larger the the MTU size send it using offer.
                if (length < publication.maxMessageLength()) {
                    tryClaim(byteBuffer, length);
                } else {
                    offer(byteBuffer, length);
                }
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
                        child.onError(new RuntimeException("not connected"));
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
                        child.onError(new RuntimeException("not connected"));
                        break;
                    }
                }
                request(1);
            }
        };
    }
}
