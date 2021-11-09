/*
 * Copyright 2015-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.rsocket.transport.aeron;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.aeron.logbuffer.BufferClaim;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.rsocket.util.NumberUtils;
import java.nio.ByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;

class PublicationUtils {

  public static boolean tryOfferSetupFrame(
      Aeron.Context context,
      ExclusivePublication publication,
      BufferClaim bufferClaim,
      IdleStrategy idleStrategy,
      String channel,
      long connectionId,
      int streamId,
      long timeoutNs,
      FrameType frameType) {

    final int lengthToClime =
        SetupCodec.constantLength()
            + NumberUtils.requireUnsignedShort(ByteBufUtil.utf8Bytes(channel));

    final NanoClock nanoClock = context.nanoClock();
    final long nowNs = nanoClock.nanoTime();
    final long deadlineNs = nowNs + timeoutNs;

    idleStrategy.reset();
    for (; ; ) {
      final long state = publication.tryClaim(lengthToClime, bufferClaim);

      if (state < 0) {
        if (state != ExclusivePublication.CLOSED) {
          return false;
        }

        idleStrategy.idle();
        if (deadlineNs - nanoClock.nanoTime() < 0) {
          return false;
        }

        continue;
      }

      SetupCodec.encode(
          bufferClaim.buffer(), bufferClaim.offset(), connectionId, streamId, channel, frameType);

      bufferClaim.commit();

      return true;
    }
  }

  public static boolean tryClaimOrOffer(
      ByteBuf content,
      ExclusivePublication publication,
      BufferClaim bufferClaim,
      UnsafeBuffer unsafeBuffer,
      int effort) {
    ByteBuffer byteBuffer = content.nioBuffer();
    boolean successful = false;
    int offset = content.readerIndex();
    int capacity = content.readableBytes();
    int maxPayloadLength = publication.maxPayloadLength();

    if (capacity < maxPayloadLength) {
      while (!successful && effort-- > 0) {
        long offer = publication.tryClaim(capacity, bufferClaim);
        if (offer >= 0) {
          try {
            final MutableDirectBuffer b = bufferClaim.buffer();
            int claimOffset = bufferClaim.offset();
            b.putBytes(claimOffset, byteBuffer, offset, capacity);
          } finally {
            bufferClaim.commit();
            successful = true;
          }
        } else {
          if (offer == Publication.CLOSED) {
            throw new NotConnectedException();
          }
        }
      }
    } else {
      unsafeBuffer.wrap(byteBuffer, offset, capacity);
      while (!successful && effort-- > 0) {
        long offer = publication.offer(unsafeBuffer);
        if (offer < 0) {
          if (offer == Publication.CLOSED) {
            throw new NotConnectedException();
          }
        } else {
          successful = true;
        }
      }
    }

    return successful;
  }
}
