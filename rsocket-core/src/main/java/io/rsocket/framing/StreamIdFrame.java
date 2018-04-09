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
 * An RSocket frame with a stream id.
 *
 * @see <a
 *     href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-header-format">Frame
 *     Header Format</a>
 */
public final class StreamIdFrame extends AbstractRecyclableFrame<StreamIdFrame> {

  private static final Recycler<StreamIdFrame> RECYCLER = createRecycler(StreamIdFrame::new);

  private static final int STREAM_ID_BYTES = Integer.BYTES;

  private StreamIdFrame(Handle<StreamIdFrame> handle) {
    super(handle);
  }

  /**
   * Creates the frame with a stream id.
   *
   * @param byteBuf the {@link ByteBuf} representing the frame
   * @return the frame with a stream id
   * @throws NullPointerException if {@code byteBuf} is {@code null}
   */
  public static StreamIdFrame createStreamIdFrame(ByteBuf byteBuf) {
    Objects.requireNonNull(byteBuf, "byteBuf must not be null");

    return RECYCLER.get().setByteBuf(byteBuf.retain());
  }

  /**
   * Creates the frame with a stream id.
   *
   * @param byteBufAllocator the {@code ByteBufAllocator} to use
   * @param streamId the stream id
   * @param frame the frame to prepend the stream id to
   * @return the frame with a stream id
   * @throws NullPointerException if {@code byteBufAllocator} or {@code frame} is {@code null}
   */
  public static StreamIdFrame createStreamIdFrame(
      ByteBufAllocator byteBufAllocator, int streamId, Frame frame) {

    Objects.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    Objects.requireNonNull(frame, "frame must not be null");

    ByteBuf streamIdByteBuf =
        frame.mapFrame(
            frameByteBuf -> {
              ByteBuf byteBuf = byteBufAllocator.buffer(STREAM_ID_BYTES).writeInt(streamId);

              return Unpooled.wrappedBuffer(byteBuf, frameByteBuf.retain());
            });

    return RECYCLER.get().setByteBuf(streamIdByteBuf);
  }

  /**
   * Returns the stream id.
   *
   * @return the stream id
   */
  public int getStreamId() {
    return getByteBuf().getInt(0);
  }

  /**
   * Returns the frame without stream id directly.
   *
   * <p><b>Note:</b> this frame without stream id will be outside of the {@link Frame}'s lifecycle
   * and may be released at any time. It is highly recommended that you {@link ByteBuf#retain()} the
   * frame without stream id if you store it.
   *
   * @return the frame without stream id directly
   * @see #mapFrameWithoutStreamId(Function)
   */
  public ByteBuf getUnsafeFrameWithoutStreamId() {
    ByteBuf byteBuf = getByteBuf();
    return byteBuf.slice(STREAM_ID_BYTES, byteBuf.readableBytes() - STREAM_ID_BYTES).asReadOnly();
  }

  /**
   * Exposes the {@link Frame} without the stream id as a {@link ByteBuf} for mapping to a different
   * type.
   *
   * @param function the function to transform the {@link Frame} without the stream id as a {@link
   *     ByteBuf} to a different type
   * @param <T> the different type
   * @return the {@link Frame} without the stream id as a {@link ByteBuf} mapped to a different type
   * @throws NullPointerException if {@code function} is {@code null}
   */
  public <T> T mapFrameWithoutStreamId(Function<ByteBuf, T> function) {
    Objects.requireNonNull(function, "function must not be null");

    return function.apply(getUnsafeFrameWithoutStreamId());
  }

  @Override
  public String toString() {
    return "StreamIdFrame{"
        + "streamId="
        + getStreamId()
        + ", frameWithoutStreamId="
        + mapFrameWithoutStreamId(ByteBufUtil::hexDump)
        + '}';
  }
}
