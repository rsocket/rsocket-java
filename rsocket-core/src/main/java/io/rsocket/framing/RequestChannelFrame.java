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
import io.rsocket.util.NumberUtils;
import java.util.Objects;
import reactor.util.annotation.Nullable;

/**
 * An RSocket {@code REQUEST_CHANNEL} frame.
 *
 * @see <a
 *     href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#request_channel-frame-0x07">Request
 *     Channel Frame</a>
 */
public final class RequestChannelFrame
    extends AbstractRecyclableFragmentableFrame<RequestChannelFrame> {

  private static final int FLAG_COMPLETE = 1 << 6;

  private static final int OFFSET_INITIAL_REQUEST_N = FRAME_TYPE_AND_FLAGS_BYTES;

  private static final int OFFSET_METADATA_LENGTH = OFFSET_INITIAL_REQUEST_N + Integer.BYTES;

  private static final Recycler<RequestChannelFrame> RECYCLER =
      createRecycler(RequestChannelFrame::new);

  private RequestChannelFrame(Handle<RequestChannelFrame> handle) {
    super(handle);
  }

  /**
   * Creates the {@code REQUEST_CHANNEL} frame.
   *
   * @param byteBuf the {@link ByteBuf} representing the frame
   * @return the {@code REQUEST_CHANNEL} frame.
   * @throws NullPointerException if {@code byteBuf} is {@code null}
   */
  public static RequestChannelFrame createRequestChannelFrame(ByteBuf byteBuf) {
    Objects.requireNonNull(byteBuf, "byteBuf must not be null");

    return RECYCLER.get().setByteBuf(byteBuf.retain());
  }

  /**
   * Creates the {@code REQUEST_CHANNEL} frame.
   *
   * @param byteBufAllocator the {@code ByteBufAllocator} to use
   * @param follows whether to set the Follows flag
   * @param complete whether to set the Complete flag
   * @param initialRequestN the initial requestN
   * @param metadata the metadata
   * @param data the data
   * @return the {@code REQUEST_CHANNEL} frame
   * @throws NullPointerException if {@code byteBufAllocator} is {@code null}
   */
  public static RequestChannelFrame createRequestChannelFrame(
      ByteBufAllocator byteBufAllocator,
      boolean follows,
      boolean complete,
      int initialRequestN,
      @Nullable String metadata,
      @Nullable String data) {

    ByteBuf metadataByteBuf = getUtf8AsByteBuf(metadata);
    ByteBuf dataByteBuf = getUtf8AsByteBuf(data);

    try {
      return createRequestChannelFrame(
          byteBufAllocator, follows, complete, initialRequestN, metadataByteBuf, dataByteBuf);
    } finally {
      release(metadataByteBuf);
      release(dataByteBuf);
    }
  }

  /**
   * Creates the {@code REQUEST_CHANNEL} frame.
   *
   * @param byteBufAllocator the {@code ByteBufAllocator} to use
   * @param follows whether to set the Follows flag
   * @param complete whether to set the Complete flag
   * @param initialRequestN the initial requestN
   * @param metadata the metadata
   * @param data the data
   * @return the {@code REQUEST_CHANNEL} frame
   * @throws NullPointerException if {@code byteBufAllocator} is {@code null}
   * @throws IllegalArgumentException if {@code initialRequestN} is not positive
   */
  public static RequestChannelFrame createRequestChannelFrame(
      ByteBufAllocator byteBufAllocator,
      boolean follows,
      boolean complete,
      int initialRequestN,
      @Nullable ByteBuf metadata,
      @Nullable ByteBuf data) {

    NumberUtils.requirePositive(initialRequestN, "initialRequestN must be positive");

    ByteBuf byteBuf = createFrameTypeAndFlags(byteBufAllocator, FrameType.REQUEST_CHANNEL);

    if (follows) {
      byteBuf = setFollowsFlag(byteBuf);
    }

    if (complete) {
      byteBuf = setFlag(byteBuf, FLAG_COMPLETE);
    }

    byteBuf = byteBuf.writeInt(initialRequestN);
    byteBuf = appendMetadata(byteBufAllocator, byteBuf, metadata);
    byteBuf = appendData(byteBuf, data);

    return RECYCLER.get().setByteBuf(byteBuf);
  }

  @Override
  public RequestChannelFrame createFragment(
      ByteBufAllocator byteBufAllocator, @Nullable ByteBuf metadata, @Nullable ByteBuf data) {

    return createRequestChannelFrame(
        byteBufAllocator, true, isCompleteFlagSet(), getInitialRequestN(), metadata, data);
  }

  @Override
  public RequestChannelFrame createNonFragment(
      ByteBufAllocator byteBufAllocator, @Nullable ByteBuf metadata, @Nullable ByteBuf data) {

    return createRequestChannelFrame(
        byteBufAllocator, false, isCompleteFlagSet(), getInitialRequestN(), metadata, data);
  }

  /**
   * Returns the initial requestN.
   *
   * @return the initial requestN
   */
  public int getInitialRequestN() {
    return getByteBuf().getInt(OFFSET_INITIAL_REQUEST_N);
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

  @Override
  public String toString() {
    return "RequestChannelFrame{"
        + "follows="
        + isFollowsFlagSet()
        + ", complete="
        + isCompleteFlagSet()
        + ", initialRequestN="
        + getInitialRequestN()
        + ", metadata="
        + mapMetadata(ByteBufUtil::hexDump)
        + ", data="
        + mapData(ByteBufUtil::hexDump)
        + '}';
  }
}
