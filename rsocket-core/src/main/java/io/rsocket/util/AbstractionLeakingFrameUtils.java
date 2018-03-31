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

package io.rsocket.util;

import static io.rsocket.framing.FrameLengthFrame.createFrameLengthFrame;
import static io.rsocket.framing.StreamIdFrame.createStreamIdFrame;
import static io.rsocket.util.DisposableUtil.disposeQuietly;

import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Frame;
import io.rsocket.framing.FrameFactory;
import io.rsocket.framing.FrameLengthFrame;
import io.rsocket.framing.StreamIdFrame;
import java.util.Objects;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

public final class AbstractionLeakingFrameUtils {

  private AbstractionLeakingFrameUtils() {}

  /**
   * Returns a {@link Tuple2} of the stream id, and the frame. This strips the frame length and
   * stream id header from the abstraction leaking frame.
   *
   * @param abstractionLeakingFrame the abstraction leaking frame
   * @return a {@link Tuple2} of the stream id, and the frame
   * @throws NullPointerException if {@code abstractionLeakingFrame} is {@code null}
   */
  public static Tuple2<Integer, io.rsocket.framing.Frame> fromAbstractionLeakingFrame(
      Frame abstractionLeakingFrame) {

    Objects.requireNonNull(abstractionLeakingFrame, "abstractionLeakingFrame must not be null");

    FrameLengthFrame frameLengthFrame = null;
    StreamIdFrame streamIdFrame = null;

    try {
      frameLengthFrame = createFrameLengthFrame(abstractionLeakingFrame.content());
      streamIdFrame =
          frameLengthFrame.mapFrameWithoutFrameLength(StreamIdFrame::createStreamIdFrame);

      io.rsocket.framing.Frame frame =
          streamIdFrame.mapFrameWithoutStreamId(FrameFactory::createFrame);

      return Tuples.of(streamIdFrame.getStreamId(), frame);
    } finally {
      disposeQuietly(frameLengthFrame, streamIdFrame);
      abstractionLeakingFrame.release();
    }
  }

  /**
   * Returns an abstraction leaking frame with the stream id and frame. This adds the frame length
   * and stream id header to the frame.
   *
   * @param byteBufAllocator the {@link ByteBufAllocator} to use
   * @param streamId the stream id
   * @param frame the frame
   * @return an abstraction leaking frame with the stream id and frame
   * @throws NullPointerException if {@code byteBufAllocator} or {@code frame} is {@code null}
   */
  public static Frame toAbstractionLeakingFrame(
      ByteBufAllocator byteBufAllocator, int streamId, io.rsocket.framing.Frame frame) {

    Objects.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    Objects.requireNonNull(frame, "frame must not be null");

    StreamIdFrame streamIdFrame = null;
    FrameLengthFrame frameLengthFrame = null;

    try {
      streamIdFrame = createStreamIdFrame(byteBufAllocator, streamId, frame);
      frameLengthFrame = createFrameLengthFrame(byteBufAllocator, streamIdFrame);

      return frameLengthFrame.mapFrame(byteBuf -> Frame.from(byteBuf.retain()));
    } finally {
      disposeQuietly(frame, streamIdFrame, frameLengthFrame);
    }
  }
}
