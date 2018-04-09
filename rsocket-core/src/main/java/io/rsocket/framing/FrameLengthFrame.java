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

package io.rsocket.framing;

import static io.rsocket.framing.LengthUtils.getLengthAsUnsignedMedium;
import static io.rsocket.util.NumberUtils.MEDIUM_BYTES;
import static io.rsocket.util.RecyclerFactory.createRecycler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import java.util.Objects;
import java.util.function.Function;

/**
 * An RSocket frame with a frame length.
 *
 * @see <a href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#framing-format">Framing
 *     Format</a>
 */
public final class FrameLengthFrame extends AbstractRecyclableFrame<FrameLengthFrame> {

  private static final int FRAME_LENGTH_BYTES = MEDIUM_BYTES;

  private static final Recycler<FrameLengthFrame> RECYCLER = createRecycler(FrameLengthFrame::new);

  private FrameLengthFrame(Handle<FrameLengthFrame> handle) {
    super(handle);
  }

  /**
   * Creates the frame with a frame length.
   *
   * @param byteBuf the {@link ByteBuf} representing the frame
   * @return the frame with a frame length
   * @throws NullPointerException if {@code byteBuf} is {@code null}
   */
  public static FrameLengthFrame createFrameLengthFrame(ByteBuf byteBuf) {
    Objects.requireNonNull(byteBuf, "byteBuf must not be null");

    return RECYCLER.get().setByteBuf(byteBuf.retain());
  }

  /**
   * Creates the frame with a frame length.
   *
   * @param byteBufAllocator the {@link ByteBufAllocator} to use
   * @param frame the frame to prepend the frame length to
   * @return the frame with a frame length
   * @throws NullPointerException if {@code byteBufAllocator} or {@code frame} is {@code null}
   */
  public static FrameLengthFrame createFrameLengthFrame(
      ByteBufAllocator byteBufAllocator, Frame frame) {

    Objects.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    Objects.requireNonNull(frame, "frame must not be null");

    ByteBuf frameLengthByteBuf =
        frame.mapFrame(
            frameByteBuf -> {
              ByteBuf byteBuf =
                  byteBufAllocator
                      .buffer(FRAME_LENGTH_BYTES)
                      .writeMedium(getLengthAsUnsignedMedium(frameByteBuf));

              return Unpooled.wrappedBuffer(byteBuf, frameByteBuf.retain());
            });

    return RECYCLER.get().setByteBuf(frameLengthByteBuf);
  }

  /**
   * Returns the frame length.
   *
   * @return the frame length
   */
  public int getFrameLength() {
    return getByteBuf().getUnsignedMedium(0);
  }

  /**
   * Returns the frame without frame length directly.
   *
   * <p><b>Note:</b> this frame without frame length will be outside of the {@link Frame}'s
   * lifecycle and may be released at any time. It is highly recommended that you {@link
   * ByteBuf#retain()} the frame without frame length if you store it.
   *
   * @return the frame without frame length directly
   * @see #mapFrameWithoutFrameLength(Function)
   */
  public ByteBuf getUnsafeFrameWithoutFrameLength() {
    ByteBuf byteBuf = getByteBuf();
    return byteBuf
        .slice(FRAME_LENGTH_BYTES, byteBuf.readableBytes() - FRAME_LENGTH_BYTES)
        .asReadOnly();
  }

  /**
   * Exposes the {@link Frame} without the frame length as a {@link ByteBuf} for mapping to a
   * different type.
   *
   * @param function the function to transform the {@link Frame} without the frame length as a
   *     {@link ByteBuf} to a different type
   * @param <T> the different type
   * @return the {@link Frame} without the frame length as a {@link ByteBuf} mapped to a different
   *     type
   * @throws NullPointerException if {@code function} is {@code null}
   */
  public <T> T mapFrameWithoutFrameLength(Function<ByteBuf, T> function) {
    Objects.requireNonNull(function, "function must not be null");

    return function.apply(getUnsafeFrameWithoutFrameLength());
  }

  @Override
  public String toString() {
    return "FrameLengthFrame{"
        + "frameLength="
        + getFrameLength()
        + ", frameWithoutFrameLength="
        + mapFrameWithoutFrameLength(ByteBufUtil::hexDump)
        + '}';
  }
}
