/*
 * Copyright 2015-2020 the original author or authors.
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
package io.rsocket.core;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.IllegalReferenceCountException;
import io.rsocket.frame.FrameHeaderCodec;
import io.rsocket.frame.FrameLengthCodec;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.PayloadFrameCodec;
import io.rsocket.frame.RequestChannelFrameCodec;
import io.rsocket.frame.RequestFireAndForgetFrameCodec;
import io.rsocket.frame.RequestResponseFrameCodec;
import io.rsocket.frame.RequestStreamFrameCodec;
import reactor.util.annotation.Nullable;

class FragmentationUtils {

  static final int MIN_MTU_SIZE = 64;

  static final int FRAME_OFFSET = // 9 bytes in total
      FrameLengthCodec.FRAME_LENGTH_SIZE // includes encoded frame length bytes size
          + FrameHeaderCodec.size(); // includes encoded frame headers info bytes size
  static final int FRAME_OFFSET_WITH_METADATA = // 12 bytes in total
      FRAME_OFFSET
          + FrameLengthCodec.FRAME_LENGTH_SIZE; // include encoded metadata length bytes size

  static final int FRAME_OFFSET_WITH_INITIAL_REQUEST_N = // 13 bytes in total
      FRAME_OFFSET + Integer.BYTES; // includes extra space for initialRequestN bytes size
  static final int FRAME_OFFSET_WITH_METADATA_AND_INITIAL_REQUEST_N = // 16 bytes in total
      FRAME_OFFSET_WITH_METADATA
          + Integer.BYTES; // includes extra space for initialRequestN bytes size

  static boolean isFragmentable(
      int mtu, ByteBuf data, @Nullable ByteBuf metadata, boolean hasInitialRequestN) {
    if (mtu == 0) {
      return false;
    }

    if (metadata != null) {
      int remaining =
          mtu
              - (hasInitialRequestN
                  ? FRAME_OFFSET_WITH_METADATA_AND_INITIAL_REQUEST_N
                  : FRAME_OFFSET_WITH_METADATA);

      return (metadata.readableBytes() + data.readableBytes()) > remaining;
    } else {
      int remaining =
          mtu - (hasInitialRequestN ? FRAME_OFFSET_WITH_INITIAL_REQUEST_N : FRAME_OFFSET);

      return data.readableBytes() > remaining;
    }
  }

  static ByteBuf encodeFollowsFragment(
      ByteBufAllocator allocator,
      int mtu,
      int streamId,
      boolean complete,
      ByteBuf metadata,
      ByteBuf data) {
    // subtract the header bytes + frame length size
    int remaining = mtu - FRAME_OFFSET;

    ByteBuf metadataFragment = null;
    if (metadata.isReadable()) {
      // subtract the metadata frame length
      remaining -= FrameLengthCodec.FRAME_LENGTH_SIZE;
      int r = Math.min(remaining, metadata.readableBytes());
      remaining -= r;
      metadataFragment = metadata.readRetainedSlice(r);
    }

    ByteBuf dataFragment = Unpooled.EMPTY_BUFFER;
    try {
      if (remaining > 0 && data.isReadable()) {
        int r = Math.min(remaining, data.readableBytes());
        dataFragment = data.readRetainedSlice(r);
      }
    } catch (IllegalReferenceCountException | NullPointerException e) {
      if (metadataFragment != null) {
        metadataFragment.release();
      }
      throw e;
    }

    boolean follows = data.isReadable() || metadata.isReadable();
    return PayloadFrameCodec.encode(
        allocator, streamId, follows, (!follows && complete), true, metadataFragment, dataFragment);
  }

  static ByteBuf encodeFirstFragment(
      ByteBufAllocator allocator,
      int mtu,
      FrameType frameType,
      int streamId,
      boolean hasMetadata,
      ByteBuf metadata,
      ByteBuf data) {
    // subtract the header bytes + frame length size
    int remaining = mtu - FRAME_OFFSET;

    ByteBuf metadataFragment = hasMetadata ? Unpooled.EMPTY_BUFFER : null;
    if (metadata.isReadable()) {
      // subtract the metadata frame length
      remaining -= FrameLengthCodec.FRAME_LENGTH_SIZE;
      int r = Math.min(remaining, metadata.readableBytes());
      remaining -= r;
      metadataFragment = metadata.readRetainedSlice(r);
    }

    ByteBuf dataFragment = Unpooled.EMPTY_BUFFER;
    try {
      if (remaining > 0 && data.isReadable()) {
        int r = Math.min(remaining, data.readableBytes());
        dataFragment = data.readRetainedSlice(r);
      }
    } catch (IllegalReferenceCountException | NullPointerException e) {
      if (metadataFragment != null) {
        metadataFragment.release();
      }
      throw e;
    }

    switch (frameType) {
      case REQUEST_FNF:
        return RequestFireAndForgetFrameCodec.encode(
            allocator, streamId, true, metadataFragment, dataFragment);
      case REQUEST_RESPONSE:
        return RequestResponseFrameCodec.encode(
            allocator, streamId, true, metadataFragment, dataFragment);
        // Payload and synthetic types from the responder side
      case PAYLOAD:
        return PayloadFrameCodec.encode(
            allocator, streamId, true, false, false, metadataFragment, dataFragment);
      case NEXT:
        // see https://github.com/rsocket/rsocket/blob/master/Protocol.md#handling-the-unexpected
        // point 7
      case NEXT_COMPLETE:
        return PayloadFrameCodec.encode(
            allocator, streamId, true, false, true, metadataFragment, dataFragment);
      default:
        throw new IllegalStateException("unsupported fragment type: " + frameType);
    }
  }

  static ByteBuf encodeFirstFragment(
      ByteBufAllocator allocator,
      int mtu,
      long initialRequestN,
      FrameType frameType,
      int streamId,
      ByteBuf metadata,
      ByteBuf data) {
    // subtract the header bytes + frame length bytes + initial requestN bytes
    int remaining = mtu - FRAME_OFFSET_WITH_INITIAL_REQUEST_N;

    ByteBuf metadataFragment = null;
    if (metadata.isReadable()) {
      // subtract the metadata frame length
      remaining -= FrameLengthCodec.FRAME_LENGTH_SIZE;
      int r = Math.min(remaining, metadata.readableBytes());
      remaining -= r;
      metadataFragment = metadata.readRetainedSlice(r);
    }

    ByteBuf dataFragment = Unpooled.EMPTY_BUFFER;
    try {
      if (remaining > 0 && data.isReadable()) {
        int r = Math.min(remaining, data.readableBytes());
        dataFragment = data.readRetainedSlice(r);
      }
    } catch (IllegalReferenceCountException | NullPointerException e) {
      if (metadataFragment != null) {
        metadataFragment.release();
      }
      throw e;
    }

    switch (frameType) {
        // Requester Side
      case REQUEST_STREAM:
        return RequestStreamFrameCodec.encode(
            allocator, streamId, true, initialRequestN, metadataFragment, dataFragment);
      case REQUEST_CHANNEL:
        return RequestChannelFrameCodec.encode(
            allocator, streamId, true, false, initialRequestN, metadataFragment, dataFragment);
      default:
        throw new IllegalStateException("unsupported fragment type: " + frameType);
    }
  }

  static int assertMtu(int mtu) {
    if (mtu > 0 && mtu < MIN_MTU_SIZE || mtu < 0) {
      String msg =
          String.format(
              "The smallest allowed mtu size is %d bytes, provided: %d", MIN_MTU_SIZE, mtu);
      throw new IllegalArgumentException(msg);
    } else {
      return mtu;
    }
  }
}
