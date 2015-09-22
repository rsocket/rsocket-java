package io.reactivesocket.aeron.internal;

import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

/**
 * Utils for dealing with Aeron
 */
public class AeronUtil {

    private static final ThreadLocal<BufferClaim> bufferClaims = ThreadLocal.withInitial(BufferClaim::new);

    private static final ThreadLocal<UnsafeBuffer> unsafeBuffers = ThreadLocal.withInitial(() -> new UnsafeBuffer(Constants.EMTPY));

    /**
     * Sends a message using offer. This method will spin-lock if Aeron signals back pressure.
     *
     * This method of sending data does not need to know how long the message is.
     *
     * @param publication publication to send the message on
     *
     * @param fillBuffer closure passed in to fill a {@link uk.co.real_logic.agrona.MutableDirectBuffer}
     *                   that is send over Aeron
     */
    public static void offer(Publication publication, BufferFiller fillBuffer) {
        final UnsafeBuffer buffer = unsafeBuffers.get();
        fillBuffer.fill(0, buffer);
        do {
            final long offer = publication.offer(buffer);
            if (offer >= 0) {
                break;
            } else if (Publication.NOT_CONNECTED == offer) {
                throw new RuntimeException("not connected");
            }
        } while(true);
    }

    /**
     * Sends a message using tryClaim. This method will spin-lock if Aeron signals back pressure. The message
     * being sent needs to be equal or smaller than Aeron's MTU size or an exception will be thrown.
     *
     * In order to use this method of sending data you need to know the length of data.
     *
     * @param publication publication to send the message on
     * @param fillBuffer closure passed in to fill a {@link uk.co.real_logic.agrona.MutableDirectBuffer}
     *                   that is send over Aeron
     * @param length the length of data
     */
    public static void tryClaim(Publication publication, BufferFiller fillBuffer, int length) {
        final BufferClaim bufferClaim = bufferClaims.get();
        do {
            final long offer = publication.tryClaim(length, bufferClaim);
            if (offer >= 0) {
                try {
                    final MutableDirectBuffer buffer = bufferClaim.buffer();
                    final int offset = bufferClaim.offset();
                    fillBuffer.fill(offset, buffer);
                } finally {
                    bufferClaim.commit();
                }
            } else if (Publication.NOT_CONNECTED == offer) {
                throw new RuntimeException("not connected");
            }
        } while (true);
    }

    /**
     * Attempts to send the data using tryClaim. If the message data length is large then the Aeron MTU
     * size it will use offer instead.
     *
     * @param publication publication to send the message on
     * @param fillBuffer closure passed in to fill a {@link uk.co.real_logic.agrona.MutableDirectBuffer}
     *                   that is send over Aeron
     * @param length the length of data
     */
    public static void tryClaimOrOffer(Publication publication, BufferFiller fillBuffer, int length) {
        try {
            if (length < Constants.AERON_MTU_SIZE) {
                tryClaim(publication, fillBuffer, length);
            } else {
                offer(publication, fillBuffer);
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    /**
     * Implement this to fill a DirectBuffer passed in by either the offer or tryClaim methods.
     */
    public interface BufferFiller {
        void fill(int offset, MutableDirectBuffer buffer);
    }
}
