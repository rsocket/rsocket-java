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

import static io.rsocket.core.FragmentationUtils.isFragmentable;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ReferenceCounted;
import io.rsocket.DuplexConnection;
import io.rsocket.Payload;
import io.rsocket.exceptions.CanceledException;
import io.rsocket.frame.CancelFrameCodec;
import io.rsocket.frame.ErrorFrameCodec;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.PayloadFrameCodec;
import io.rsocket.frame.RequestChannelFrameCodec;
import io.rsocket.frame.RequestFireAndForgetFrameCodec;
import io.rsocket.frame.RequestResponseFrameCodec;
import io.rsocket.frame.RequestStreamFrameCodec;
import java.util.function.Consumer;
import reactor.core.publisher.Operators;
import reactor.util.context.Context;

final class SendUtils {
  private static final Consumer<?> DROPPED_ELEMENTS_CONSUMER =
      data -> {
        try {
          ReferenceCounted referenceCounted = (ReferenceCounted) data;
          referenceCounted.release();
        } catch (Throwable e) {
          // ignored
        }
      };

  static final Context DISCARD_CONTEXT = Operators.enableOnDiscard(null, DROPPED_ELEMENTS_CONSUMER);

  static void sendReleasingPayload(
      int streamId,
      FrameType frameType,
      int mtu,
      Payload payload,
      DuplexConnection connection,
      ByteBufAllocator allocator,
      boolean requester) {

    final boolean hasMetadata = payload.hasMetadata();
    final ByteBuf metadata = hasMetadata ? payload.metadata() : null;
    final ByteBuf data = payload.data();

    boolean fragmentable;
    try {
      fragmentable = isFragmentable(mtu, data, metadata, false);
    } catch (IllegalReferenceCountException | NullPointerException e) {
      sendTerminalFrame(streamId, frameType, connection, allocator, requester, false, e);
      throw e;
    }

    if (fragmentable) {
      final ByteBuf slicedData = data.slice();
      final ByteBuf slicedMetadata = hasMetadata ? metadata.slice() : Unpooled.EMPTY_BUFFER;

      final ByteBuf first;
      try {
        first =
            FragmentationUtils.encodeFirstFragment(
                allocator, mtu, frameType, streamId, hasMetadata, slicedMetadata, slicedData);
      } catch (IllegalReferenceCountException e) {
        sendTerminalFrame(streamId, frameType, connection, allocator, requester, false, e);
        throw e;
      }

      connection.sendFrame(streamId, first, false);

      boolean complete = frameType == FrameType.NEXT_COMPLETE;
      while (slicedData.isReadable() || slicedMetadata.isReadable()) {
        final ByteBuf following;
        try {
          following =
              FragmentationUtils.encodeFollowsFragment(
                  allocator, mtu, streamId, complete, slicedMetadata, slicedData);
        } catch (IllegalReferenceCountException e) {
          sendTerminalFrame(streamId, frameType, connection, allocator, requester, true, e);
          throw e;
        }
        connection.sendFrame(streamId, following, false);
      }

      try {
        payload.release();
      } catch (IllegalReferenceCountException e) {
        sendTerminalFrame(streamId, frameType, connection, allocator, true, true, e);
        throw e;
      }
    } else {
      final ByteBuf dataRetainedSlice = data.retainedSlice();

      final ByteBuf metadataRetainedSlice;
      try {
        metadataRetainedSlice = hasMetadata ? metadata.retainedSlice() : null;
      } catch (IllegalReferenceCountException e) {
        dataRetainedSlice.release();

        sendTerminalFrame(streamId, frameType, connection, allocator, requester, false, e);
        throw e;
      }

      try {
        payload.release();
      } catch (IllegalReferenceCountException e) {
        dataRetainedSlice.release();
        if (hasMetadata) {
          metadataRetainedSlice.release();
        }

        sendTerminalFrame(streamId, frameType, connection, allocator, requester, false, e);
        throw e;
      }

      final ByteBuf requestFrame;
      switch (frameType) {
        case REQUEST_FNF:
          requestFrame =
              RequestFireAndForgetFrameCodec.encode(
                  allocator, streamId, false, metadataRetainedSlice, dataRetainedSlice);
          break;
        case REQUEST_RESPONSE:
          requestFrame =
              RequestResponseFrameCodec.encode(
                  allocator, streamId, false, metadataRetainedSlice, dataRetainedSlice);
          break;
        case PAYLOAD:
        case NEXT:
        case NEXT_COMPLETE:
          requestFrame =
              PayloadFrameCodec.encode(
                  allocator,
                  streamId,
                  false,
                  frameType == FrameType.NEXT_COMPLETE,
                  frameType != FrameType.PAYLOAD,
                  metadataRetainedSlice,
                  dataRetainedSlice);
          break;
        default:
          throw new IllegalArgumentException("Unsupported frame type " + frameType);
      }

      connection.sendFrame(streamId, requestFrame, false);
    }
  }

  static void sendReleasingPayload(
      int streamId,
      FrameType frameType,
      long initialRequestN,
      int mtu,
      Payload payload,
      DuplexConnection connection,
      ByteBufAllocator allocator,
      boolean complete) {

    final boolean hasMetadata = payload.hasMetadata();
    final ByteBuf metadata = hasMetadata ? payload.metadata() : null;
    final ByteBuf data = payload.data();

    boolean fragmentable;
    try {
      fragmentable = isFragmentable(mtu, data, metadata, true);
    } catch (IllegalReferenceCountException | NullPointerException e) {
      sendTerminalFrame(streamId, frameType, connection, allocator, true, false, e);
      throw e;
    }

    if (fragmentable) {
      final ByteBuf slicedData = data.slice();
      final ByteBuf slicedMetadata = hasMetadata ? metadata.slice() : Unpooled.EMPTY_BUFFER;

      final ByteBuf first;
      try {
        first =
            FragmentationUtils.encodeFirstFragment(
                allocator,
                mtu,
                initialRequestN,
                frameType,
                streamId,
                hasMetadata,
                slicedMetadata,
                slicedData);
      } catch (IllegalReferenceCountException e) {
        sendTerminalFrame(streamId, frameType, connection, allocator, true, false, e);
        throw e;
      }

      connection.sendFrame(streamId, first, false);

      while (slicedData.isReadable() || slicedMetadata.isReadable()) {
        final ByteBuf following;
        try {
          following =
              FragmentationUtils.encodeFollowsFragment(
                  allocator, mtu, streamId, complete, slicedMetadata, slicedData);
        } catch (IllegalReferenceCountException e) {
          sendTerminalFrame(streamId, frameType, connection, allocator, true, true, e);
          throw e;
        }
        connection.sendFrame(streamId, following, false);
      }

      try {
        payload.release();
      } catch (IllegalReferenceCountException e) {
        sendTerminalFrame(streamId, frameType, connection, allocator, true, true, e);
        throw e;
      }
    } else {
      final ByteBuf dataRetainedSlice = data.retainedSlice();

      final ByteBuf metadataRetainedSlice;
      try {
        metadataRetainedSlice = hasMetadata ? metadata.retainedSlice() : null;
      } catch (IllegalReferenceCountException e) {
        dataRetainedSlice.release();

        sendTerminalFrame(streamId, frameType, connection, allocator, true, false, e);
        throw e;
      }

      try {
        payload.release();
      } catch (IllegalReferenceCountException e) {
        dataRetainedSlice.release();
        if (hasMetadata) {
          metadataRetainedSlice.release();
        }

        sendTerminalFrame(streamId, frameType, connection, allocator, true, false, e);
        throw e;
      }

      final ByteBuf requestFrame;
      switch (frameType) {
        case REQUEST_STREAM:
          requestFrame =
              RequestStreamFrameCodec.encode(
                  allocator,
                  streamId,
                  false,
                  initialRequestN,
                  metadataRetainedSlice,
                  dataRetainedSlice);
          break;
        case REQUEST_CHANNEL:
          requestFrame =
              RequestChannelFrameCodec.encode(
                  allocator,
                  streamId,
                  false,
                  complete,
                  initialRequestN,
                  metadataRetainedSlice,
                  dataRetainedSlice);
          break;
        default:
          throw new IllegalArgumentException("Unsupported frame type " + frameType);
      }

      connection.sendFrame(streamId, requestFrame, false);
    }
  }

  static void sendTerminalFrame(
      int streamId,
      FrameType frameType,
      DuplexConnection connection,
      ByteBufAllocator allocator,
      boolean requester,
      boolean onFollowingFrame,
      Throwable t) {

    if (onFollowingFrame) {
      if (requester) {
        final ByteBuf cancelFrame = CancelFrameCodec.encode(allocator, streamId);
        connection.sendFrame(streamId, cancelFrame, false);
      } else {
        final ByteBuf errorFrame =
            ErrorFrameCodec.encode(
                allocator,
                streamId,
                new CanceledException(
                    "Failed to encode fragmented "
                        + frameType
                        + " frame. Cause: "
                        + t.getMessage()));
        connection.sendFrame(streamId, errorFrame, false);
      }
    } else {
      switch (frameType) {
        case NEXT_COMPLETE:
        case NEXT:
        case PAYLOAD:
          if (requester) {
            final ByteBuf cancelFrame = CancelFrameCodec.encode(allocator, streamId);
            connection.sendFrame(streamId, cancelFrame, false);
          } else {
            final ByteBuf errorFrame =
                ErrorFrameCodec.encode(
                    allocator,
                    streamId,
                    new CanceledException(
                        "Failed to encode " + frameType + " frame. Cause: " + t.getMessage()));
            connection.sendFrame(streamId, errorFrame, false);
          }
      }
    }
  }
}
