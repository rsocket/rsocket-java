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

import static io.netty.util.ReferenceCountUtil.release;
import static io.rsocket.framing.FrameType.ERROR;
import static io.rsocket.util.RecyclerFactory.createRecycler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import java.util.Objects;
import reactor.util.annotation.Nullable;

/**
 * An RSocket {@code ERROR} frame.
 *
 * @see <a href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#error-frame-0x0b">Error
 *     Frame</a>
 */
public final class ErrorFrame extends AbstractRecyclableDataFrame<ErrorFrame> {

  private static final int OFFSET_ERROR_CODE = FRAME_TYPE_AND_FLAGS_BYTES;

  private static final int OFFSET_DATA = OFFSET_ERROR_CODE + Integer.BYTES;

  private static final Recycler<ErrorFrame> RECYCLER = createRecycler(ErrorFrame::new);

  private ErrorFrame(Handle<ErrorFrame> handle) {
    super(handle);
  }

  /**
   * Creates the {@code ERROR} frame.
   *
   * @param byteBuf the {@link ByteBuf} representing the frame
   * @return the {@code ERROR} frame.
   * @throws NullPointerException if {@code byteBuf} is {@code null}
   */
  public static ErrorFrame createErrorFrame(ByteBuf byteBuf) {
    Objects.requireNonNull(byteBuf, "byteBuf must not be null");

    return RECYCLER.get().setByteBuf(byteBuf.retain());
  }

  /**
   * Creates the {@code ERROR} frame.
   *
   * @param byteBufAllocator the {@code ByteBufAllocator} to use
   * @param errorCode the error code
   * @param data the error data
   * @return the {@code ERROR} frame
   * @throws NullPointerException if {@code byteBufAllocator} is {@code null}
   */
  public static ErrorFrame createErrorFrame(
      ByteBufAllocator byteBufAllocator, int errorCode, @Nullable String data) {

    ByteBuf dataByteBuf = getUtf8AsByteBuf(data);

    try {
      return createErrorFrame(byteBufAllocator, errorCode, dataByteBuf);
    } finally {
      release(dataByteBuf);
    }
  }

  /**
   * Creates the {@code ERROR} frame.
   *
   * @param byteBufAllocator the {@code ByteBufAllocator} to use
   * @param errorCode the error code
   * @param data the error data
   * @return the {@code ERROR} frame
   * @throws NullPointerException if {@code byteBufAllocator} is {@code null}
   */
  public static ErrorFrame createErrorFrame(
      ByteBufAllocator byteBufAllocator, int errorCode, @Nullable ByteBuf data) {

    ByteBuf byteBuf = createFrameTypeAndFlags(byteBufAllocator, ERROR).writeInt(errorCode);
    byteBuf = appendData(byteBuf, data);

    return RECYCLER.get().setByteBuf(byteBuf);
  }

  /**
   * Returns the error code.
   *
   * @return the error code
   */
  public int getErrorCode() {
    return getByteBuf().getInt(OFFSET_ERROR_CODE);
  }

  @Override
  public ByteBuf getUnsafeData() {
    return getData(OFFSET_DATA);
  }

  @Override
  public String toString() {
    return "ErrorFrame{"
        + "errorCode="
        + getErrorCode()
        + ", data="
        + mapData(ByteBufUtil::hexDump)
        + '}';
  }
}
