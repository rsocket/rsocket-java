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
import static io.rsocket.util.RecyclerFactory.createRecycler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import java.util.Objects;
import reactor.util.annotation.Nullable;

/**
 * An RSocket {@code PAYLOAD} frame.
 *
 * @see <a
 *     href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#payload-frame-0x0a">Payload
 *     Frame</a>
 */
public final class PayloadFrame extends AbstractRecyclableFragmentableFrame<PayloadFrame> {

  private static final int FLAG_COMPLETE = 1 << 6;

  private static final int FLAG_NEXT = 1 << 5;

  private static final int OFFSET_METADATA_LENGTH = FRAME_TYPE_AND_FLAGS_BYTES;

  private static final Recycler<PayloadFrame> RECYCLER = createRecycler(PayloadFrame::new);

  private PayloadFrame(Handle<PayloadFrame> handle) {
    super(handle);
  }

  /**
   * Creates the {@code PAYLOAD} frame.
   *
   * @param byteBuf the {@link ByteBuf} representing the frame
   * @return the {@code PAYLOAD} frame.
   * @throws NullPointerException if {@code byteBuf} is {@code null}
   */
  public static PayloadFrame createPayloadFrame(ByteBuf byteBuf) {
    Objects.requireNonNull(byteBuf, "byteBuf must not be null");

    return RECYCLER.get().setByteBuf(byteBuf.retain());
  }

  /**
   * Creates the {@code PAYLOAD} frame.
   *
   * @param byteBufAllocator the {@code ByteBufAllocator} to use
   * @param follows whether to set the Follows flag
   * @param complete respond whether to set the Complete flag
   * @param metadata the metadata
   * @param data the data
   * @return the {@code PAYLOAD} frame
   * @throws NullPointerException if {@code byteBufAllocator} is {@code null}
   */
  public static PayloadFrame createPayloadFrame(
      ByteBufAllocator byteBufAllocator,
      boolean follows,
      boolean complete,
      @Nullable String metadata,
      @Nullable String data) {

    ByteBuf metadataByteBuf = getUtf8AsByteBuf(metadata);
    ByteBuf dataByteBuf = getUtf8AsByteBuf(data);

    try {
      return createPayloadFrame(byteBufAllocator, follows, complete, metadataByteBuf, dataByteBuf);
    } finally {
      release(metadataByteBuf);
      release(dataByteBuf);
    }
  }

  /**
   * Creates the {@code PAYLOAD} frame.
   *
   * @param byteBufAllocator the {@code ByteBufAllocator} to use
   * @param follows respond whether to set the Follows flag
   * @param complete respond whether to set the Complete flag
   * @param metadata the metadata
   * @param data the data
   * @return the {@code PAYLOAD} frame
   * @throws NullPointerException if {@code byteBufAllocator} is {@code null}
   */
  public static PayloadFrame createPayloadFrame(
      ByteBufAllocator byteBufAllocator,
      boolean follows,
      boolean complete,
      @Nullable ByteBuf metadata,
      @Nullable ByteBuf data) {

    if (!complete && (data == null)) {
      throw new IllegalArgumentException(
          "Payload frame must either be complete, have data, or both");
    }

    ByteBuf byteBuf = createFrameTypeAndFlags(byteBufAllocator, FrameType.PAYLOAD);

    if (follows) {
      byteBuf = setFollowsFlag(byteBuf);
    }

    if (complete) {
      byteBuf = setFlag(byteBuf, FLAG_COMPLETE);
    }

    byteBuf = appendMetadata(byteBufAllocator, byteBuf, metadata);

    if (data != null) {
      byteBuf = setFlag(byteBuf, FLAG_NEXT);
    }

    byteBuf = appendData(byteBuf, data);

    return RECYCLER.get().setByteBuf(byteBuf);
  }

  @Override
  public PayloadFrame createFragment(
      ByteBufAllocator byteBufAllocator, @Nullable ByteBuf metadata, @Nullable ByteBuf data) {

    return createPayloadFrame(byteBufAllocator, true, isCompleteFlagSet(), metadata, data);
  }

  @Override
  public PayloadFrame createNonFragment(
      ByteBufAllocator byteBufAllocator, @Nullable ByteBuf metadata, @Nullable ByteBuf data) {

    return createPayloadFrame(byteBufAllocator, false, isCompleteFlagSet(), metadata, data);
  }

  @Override
  public ByteBuf getUnsafeData() {
    return getData(OFFSET_METADATA_LENGTH);
  }

  @Override
  public @Nullable ByteBuf getUnsafeMetadata() {
    return getMetadata(OFFSET_METADATA_LENGTH);
  }

  /**
   * Returns whether the Complete flag is set.
   *
   * @return whether the Complete flag is set
   */
  public boolean isCompleteFlagSet() {
    return isFlagSet(FLAG_COMPLETE);
  }

  /**
   * Returns whether the Next flag is set.
   *
   * @return whether the Next flag is set
   */
  public boolean isNextFlagSet() {
    return isFlagSet(FLAG_NEXT);
  }

  @Override
  public String toString() {
    return "PayloadFrame{"
        + "follows="
        + isFollowsFlagSet()
        + ", complete="
        + isCompleteFlagSet()
        + ", next="
        + isNextFlagSet()
        + ", metadata="
        + mapMetadata(ByteBufUtil::hexDump)
        + ", data="
        + mapData(ByteBufUtil::hexDump)
        + '}';
  }
}
