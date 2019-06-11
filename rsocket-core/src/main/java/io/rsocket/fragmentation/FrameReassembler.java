/*
 * Copyright 2015-2018 the original author or authors.
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

package io.rsocket.fragmentation;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;
import io.rsocket.frame.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.SynchronousSink;

/**
 * The implementation of the RSocket reassembly behavior.
 *
 * @see <a
 *     href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#fragmentation-and-reassembly">Fragmentation
 *     and Reassembly</a>
 */
final class FrameReassembler extends AtomicBoolean implements Disposable {
  private static final Logger logger = LoggerFactory.getLogger(FrameReassembler.class);

  final IntObjectMap<ByteBuf> headers;
  final IntObjectMap<CompositeByteBuf> metadata;
  final IntObjectMap<CompositeByteBuf> data;

  private final ByteBufAllocator allocator;

  public FrameReassembler(ByteBufAllocator allocator) {
    this.allocator = allocator;
    this.headers = new IntObjectHashMap<>();
    this.metadata = new IntObjectHashMap<>();
    this.data = new IntObjectHashMap<>();
  }

  @Override
  public void dispose() {
    if (compareAndSet(false, true)) {
      synchronized (FrameReassembler.this) {
        for (ByteBuf byteBuf : headers.values()) {
          ReferenceCountUtil.safeRelease(byteBuf);
        }
        headers.clear();

        for (ByteBuf byteBuf : metadata.values()) {
          ReferenceCountUtil.safeRelease(byteBuf);
        }
        metadata.clear();

        for (ByteBuf byteBuf : data.values()) {
          ReferenceCountUtil.safeRelease(byteBuf);
        }
        data.clear();
      }
    }
  }

  @Override
  public boolean isDisposed() {
    return get();
  }

  synchronized ByteBuf getHeader(int streamId) {
    return headers.get(streamId);
  }

  synchronized CompositeByteBuf getMetadata(int streamId) {
    CompositeByteBuf byteBuf = metadata.get(streamId);

    if (byteBuf == null) {
      byteBuf = allocator.compositeBuffer();
      metadata.put(streamId, byteBuf);
    }

    return byteBuf;
  }

  synchronized CompositeByteBuf getData(int streamId) {
    CompositeByteBuf byteBuf = data.get(streamId);

    if (byteBuf == null) {
      byteBuf = allocator.compositeBuffer();
      data.put(streamId, byteBuf);
    }

    return byteBuf;
  }

  synchronized ByteBuf removeHeader(int streamId) {
    return headers.remove(streamId);
  }

  synchronized CompositeByteBuf removeMetadata(int streamId) {
    return metadata.remove(streamId);
  }

  synchronized CompositeByteBuf removeData(int streamId) {
    return data.remove(streamId);
  }

  synchronized void putHeader(int streamId, ByteBuf header) {
    headers.put(streamId, header);
  }

  void cancelAssemble(int streamId) {
    ByteBuf header = removeHeader(streamId);
    CompositeByteBuf metadata = removeMetadata(streamId);
    CompositeByteBuf data = removeData(streamId);

    if (header != null) {
      ReferenceCountUtil.safeRelease(header);
    }

    if (metadata != null) {
      ReferenceCountUtil.safeRelease(metadata);
    }

    if (data != null) {
      ReferenceCountUtil.safeRelease(data);
    }
  }

  void handleNoFollowsFlag(ByteBuf frame, SynchronousSink<ByteBuf> sink, int streamId) {
    ByteBuf header = removeHeader(streamId);
    if (header != null) {
      if (FrameHeaderFlyweight.hasMetadata(header)) {
        ByteBuf assembledFrame = assembleFrameWithMetadata(frame, streamId, header);
        sink.next(assembledFrame);
      } else {
        ByteBuf data = assembleData(frame, streamId);
        ByteBuf assembledFrame = FragmentationFlyweight.encode(allocator, header, data);
        sink.next(assembledFrame);
      }
      frame.release();
    } else {
      sink.next(frame);
    }
  }

  void handleFollowsFlag(ByteBuf frame, int streamId, FrameType frameType) {
    ByteBuf header = getHeader(streamId);
    if (header == null) {
      header = frame.copy(frame.readerIndex(), FrameHeaderFlyweight.size());

      if (frameType == FrameType.REQUEST_CHANNEL || frameType == FrameType.REQUEST_STREAM) {
        int i = RequestChannelFrameFlyweight.initialRequestN(frame);
        header.writeInt(i);
      }
      putHeader(streamId, header);
    }

    if (FrameHeaderFlyweight.hasMetadata(frame)) {
      CompositeByteBuf metadata = getMetadata(streamId);
      switch (frameType) {
        case REQUEST_FNF:
          metadata.addComponents(true, RequestFireAndForgetFrameFlyweight.metadata(frame).retain());
          break;
        case REQUEST_STREAM:
          metadata.addComponents(true, RequestStreamFrameFlyweight.metadata(frame).retain());
          break;
        case REQUEST_RESPONSE:
          metadata.addComponents(true, RequestResponseFrameFlyweight.metadata(frame).retain());
          break;
        case REQUEST_CHANNEL:
          metadata.addComponents(true, RequestChannelFrameFlyweight.metadata(frame).retain());
          break;
          // Payload and synthetic types
        case PAYLOAD:
        case NEXT:
        case NEXT_COMPLETE:
        case COMPLETE:
          metadata.addComponents(true, PayloadFrameFlyweight.metadata(frame).retain());
          break;
        default:
          throw new IllegalStateException("unsupported fragment type");
      }
    }

    ByteBuf data;
    switch (frameType) {
      case REQUEST_FNF:
        data = RequestFireAndForgetFrameFlyweight.data(frame).retain();
        break;
      case REQUEST_STREAM:
        data = RequestStreamFrameFlyweight.data(frame).retain();
        break;
      case REQUEST_RESPONSE:
        data = RequestResponseFrameFlyweight.data(frame).retain();
        break;
      case REQUEST_CHANNEL:
        data = RequestChannelFrameFlyweight.data(frame).retain();
        break;
        // Payload and synthetic types
      case PAYLOAD:
      case NEXT:
      case NEXT_COMPLETE:
      case COMPLETE:
        data = PayloadFrameFlyweight.data(frame).retain();
        break;
      default:
        throw new IllegalStateException("unsupported fragment type");
    }

    getData(streamId).addComponents(true, data);
    frame.release();
  }

  void reassembleFrame(ByteBuf frame, SynchronousSink<ByteBuf> sink) {
    try {
      FrameType frameType = FrameHeaderFlyweight.frameType(frame);
      int streamId = FrameHeaderFlyweight.streamId(frame);
      switch (frameType) {
        case CANCEL:
        case ERROR:
          cancelAssemble(streamId);
        default:
      }

      if (!frameType.isFragmentable()) {
        sink.next(frame);
        return;
      }

      boolean hasFollows = FrameHeaderFlyweight.hasFollows(frame);

      if (hasFollows) {
        handleFollowsFlag(frame, streamId, frameType);
      } else {
        handleNoFollowsFlag(frame, sink, streamId);
      }

    } catch (Throwable t) {
      logger.error("error reassemble frame", t);
      sink.error(t);
    }
  }

  private ByteBuf assembleFrameWithMetadata(ByteBuf frame, int streamId, ByteBuf header) {
    ByteBuf metadata;
    CompositeByteBuf cm = removeMetadata(streamId);
    if (cm != null) {
      metadata = cm.addComponents(true, PayloadFrameFlyweight.metadata(frame).retain());
    } else {
      metadata = PayloadFrameFlyweight.metadata(frame).retain();
    }

    ByteBuf data = assembleData(frame, streamId);

    return FragmentationFlyweight.encode(allocator, header, metadata, data);
  }

  private ByteBuf assembleData(ByteBuf frame, int streamId) {
    ByteBuf data;
    CompositeByteBuf cd = removeData(streamId);
    if (cd != null) {
      cd.addComponents(true, PayloadFrameFlyweight.data(frame).retain());
      data = cd;
    } else {
      data = Unpooled.EMPTY_BUFFER;
    }

    return data;
  }
}
