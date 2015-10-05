/**
 * Copyright 2015 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.reactivesocket.aeron.internal;

import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.OneToOneConcurrentArrayQueue;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.util.concurrent.TimeUnit;

/**
 * Utils for dealing with Aeron
 */
public class AeronUtil implements Loggable {

    private static final ThreadLocal<BufferClaim> bufferClaims = ThreadLocal.withInitial(BufferClaim::new);

    private static final ThreadLocal<OneToOneConcurrentArrayQueue<MutableDirectBuffer>> unsafeBuffers
        = ThreadLocal.withInitial(() -> new OneToOneConcurrentArrayQueue<>(16));

    /**
     * Sends a message using offer. This method will spin-lock if Aeron signals back pressure.
     * <p>
     * This method of sending data does need to know how long the message is.
     *
     * @param publication publication to send the message on
     * @param fillBuffer  closure passed in to fill a {@link uk.co.real_logic.agrona.MutableDirectBuffer}
     *                    that is send over Aeron
     */
    public static void offer(Publication publication, BufferFiller fillBuffer, int length, int timeout, TimeUnit timeUnit) {
        final MutableDirectBuffer buffer = getDirectBuffer(length);
        fillBuffer.fill(0, buffer);
        final long start = System.nanoTime();
        do {
            if (timeout > 0) {
                final long current = System.nanoTime();
                if ((current - start) > timeUnit.toNanos(timeout)) {
                    throw new TimedOutException();
                }
            }
            final long offer = publication.offer(buffer);
            if (offer >= 0) {
                break;
            } else if (Publication.NOT_CONNECTED == offer) {
                throw new NotConnectedException();
            }
        } while (true);

        recycleDirectBuffer(buffer);
    }

    /**
     * Sends a message using tryClaim. This method will spin-lock if Aeron signals back pressure. The message
     * being sent needs to be equal or smaller than Aeron's MTU size or an exception will be thrown.
     * <p>
     * In order to use this method of sending data you need to know the length of data.
     *
     * @param publication publication to send the message on
     * @param fillBuffer  closure passed in to fill a {@link uk.co.real_logic.agrona.MutableDirectBuffer}
     *                    that is send over Aeron
     * @param length      the length of data
     */
    public static void tryClaim(Publication publication, BufferFiller fillBuffer, int length, int timeout, TimeUnit timeUnit) {
        final BufferClaim bufferClaim = bufferClaims.get();
        final long start = System.nanoTime();
        do {
            if (timeout > 0) {
                final long current = System.nanoTime();
                if ((current - start) > timeUnit.toNanos(timeout)) {
                    throw new TimedOutException();
                }
            }

            final long offer = publication.tryClaim(length, bufferClaim);
            if (offer >= 0) {
                try {
                    final MutableDirectBuffer buffer = bufferClaim.buffer();
                    final int offset = bufferClaim.offset();
                    fillBuffer.fill(offset, buffer);
                    break;
                } finally {
                    bufferClaim.commit();
                }
            } else if (Publication.NOT_CONNECTED == offer) {
                throw new NotConnectedException();
            }
        } while (true);
    }

    /**
     * Attempts to send the data using tryClaim. If the message data length is large then the Aeron MTU
     * size it will use offer instead.
     *
     * @param publication publication to send the message on
     * @param fillBuffer  closure passed in to fill a {@link uk.co.real_logic.agrona.MutableDirectBuffer}
     *                    that is send over Aeron
     * @param length      the length of data
     */
    public static void tryClaimOrOffer(Publication publication, BufferFiller fillBuffer, int length) {
        tryClaimOrOffer(publication, fillBuffer, length, -1, null);
    }

    public static void tryClaimOrOffer(Publication publication, BufferFiller fillBuffer, int length, int timeout, TimeUnit timeUnit) {
        if (length < Constants.AERON_MTU_SIZE) {
            tryClaim(publication, fillBuffer, length, timeout, timeUnit);
        } else {
            offer(publication, fillBuffer, length, timeout, timeUnit);
        }
    }


    /**
     * Try to get a MutableDirectBuffer from a thread-safe pool for a given length. If the buffer found
     * is bigger then the buffer in the pool creates a new buffer. If no buffer is found creates a new buffer
     *
     * @param length the requested length
     * @return either a new MutableDirectBuffer or a recycled one that has the capacity to hold the data from the old one
     */
    public static MutableDirectBuffer getDirectBuffer(int length) {
        OneToOneConcurrentArrayQueue<MutableDirectBuffer> queue = unsafeBuffers.get();
        MutableDirectBuffer buffer = queue.poll();

        if (buffer != null && buffer.capacity() < length) {
            return buffer;
        } else {
            byte[] bytes = new byte[length];
            buffer = new UnsafeBuffer(bytes);
            return buffer;
        }
    }

    /**
     * Sends a DirectBuffer back to the thread pools to be recycled.
     *
     * @param directBuffer the DirectBuffer to recycle
     */
    public static void recycleDirectBuffer(MutableDirectBuffer directBuffer) {
        OneToOneConcurrentArrayQueue<MutableDirectBuffer> queue = unsafeBuffers.get();
        queue.offer(directBuffer);
    }

    /**
     * Implement this to fill a DirectBuffer passed in by either the offer or tryClaim methods.
     */
    public interface BufferFiller {
        void fill(int offset, MutableDirectBuffer buffer);
    }
}
