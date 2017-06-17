/*
 * Copyright 2016 Netflix, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.rsocket.fragmentation;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.rsocket.Frame;
import io.rsocket.FrameType;
import io.rsocket.frame.FrameHeaderFlyweight;
import reactor.core.Disposable;

/** Assembles Fragmented frames. */
public class FrameReassembler implements Disposable {
  private final FrameType frameType;
  private final int streamId;
  private final int flags;
  private final CompositeByteBuf dataBuffer;
  private final CompositeByteBuf metadataBuffer;

  public FrameReassembler(Frame frame) {
    this.frameType = frame.getType();
    this.streamId = frame.getStreamId();
    this.flags = frame.flags();
    dataBuffer = PooledByteBufAllocator.DEFAULT.compositeBuffer();
    metadataBuffer = PooledByteBufAllocator.DEFAULT.compositeBuffer();
  }

  public synchronized void append(Frame frame) {
    final ByteBuf byteBuf = frame.content();
    final FrameType frameType = FrameHeaderFlyweight.frameType(byteBuf);
    final int frameLength = FrameHeaderFlyweight.frameLength(byteBuf);
    final int metadataLength = FrameHeaderFlyweight.metadataLength(byteBuf, frameType, frameLength);
    final int dataLength = FrameHeaderFlyweight.dataLength(byteBuf, frameType);
    if (0 < metadataLength) {
      int metadataOffset = FrameHeaderFlyweight.metadataOffset(byteBuf);
      if (FrameHeaderFlyweight.hasMetadataLengthField(frameType)) {
        metadataOffset += FrameHeaderFlyweight.FRAME_LENGTH_SIZE;
      }
      metadataBuffer.addComponent(true, byteBuf.retainedSlice(metadataOffset, metadataLength));
    }
    if (0 < dataLength) {
      final int dataOffset = FrameHeaderFlyweight.dataOffset(byteBuf, frameType, frameLength);
      dataBuffer.addComponent(true, byteBuf.retainedSlice(dataOffset, dataLength));
    }
  }

  public synchronized Frame reassemble() {
    return Frame.PayloadFrame.from(streamId, frameType, metadataBuffer, dataBuffer, flags);
  }

  @Override
  public void dispose() {
    dataBuffer.release();
    metadataBuffer.release();
  }
}
