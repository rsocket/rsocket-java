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
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import java.util.Objects;
import reactor.util.annotation.Nullable;

/**
 * An RSocket {@code REQUEST_RESPONSE} frame.
 *
 * @see <a
 *     href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#request_response-frame-0x04">Request
 *     Response Frame</a>
 */
public final class RequestResponseFrame
    extends AbstractRecyclableFragmentableFrame<RequestResponseFrame> {

  private static final int OFFSET_METADATA_LENGTH = FRAME_TYPE_AND_FLAGS_BYTES;

  private static final Recycler<RequestResponseFrame> RECYCLER =
      createRecycler(RequestResponseFrame::new);

  private RequestResponseFrame(Handle<RequestResponseFrame> handle) {
    super(handle);
  }

  /**
   * Creates the {@code REQUEST_RESPONSE} frame.
   *
   * @param byteBuf the {@link ByteBuf} representing the frame
   * @return the {@code REQUEST_RESPONSE} frame.
   * @throws NullPointerException if {@code byteBuf} is {@code null}
   */
  public static RequestResponseFrame createRequestResponseFrame(ByteBuf byteBuf) {
    Objects.requireNonNull(byteBuf, "byteBuf must not be null");

    return RECYCLER.get().setByteBuf(byteBuf.retain());
  }

  /**
   * Creates the {@code REQUEST_RESPONSE} frame.
   *
   * @param byteBufAllocator the {@code ByteBufAllocator} to use
   * @param follows whether to set the Follows flag
   * @param metadata the metadata
   * @param data the data
   * @return the {@code REQUEST_RESPONSE} frame
   * @throws NullPointerException if {@code byteBufAllocator} is {@code null}
   */
  public static RequestResponseFrame createRequestResponseFrame(
      ByteBufAllocator byteBufAllocator,
      boolean follows,
      @Nullable String metadata,
      @Nullable String data) {

    return createRequestResponseFrame(
        byteBufAllocator, follows, getUtf8AsByteBuf(metadata), getUtf8AsByteBuf(data));
  }

  /**
   * Creates the {@code REQUEST_RESPONSE} frame.
   *
   * @param byteBufAllocator the {@code ByteBufAllocator} to use
   * @param follows whether to set the Follows flag
   * @param metadata the metadata
   * @param data the data
   * @return the {@code REQUEST_RESPONSE} frame
   * @throws NullPointerException if {@code byteBufAllocator} is {@code null}
   */
  public static RequestResponseFrame createRequestResponseFrame(
      ByteBufAllocator byteBufAllocator,
      boolean follows,
      @Nullable ByteBuf metadata,
      @Nullable ByteBuf data) {

    ByteBuf byteBuf = createFrameTypeAndFlags(byteBufAllocator, FrameType.REQUEST_RESPONSE);

    if (follows) {
      byteBuf = setFollowsFlag(byteBuf);
    }

    byteBuf = appendMetadata(byteBufAllocator, byteBuf, metadata);
    byteBuf = appendData(byteBuf, data);

    return RECYCLER.get().setByteBuf(byteBuf);
  }

  @Override
  public RequestResponseFrame createFragment(
      ByteBufAllocator byteBufAllocator, @Nullable ByteBuf metadata, @Nullable ByteBuf data) {

    return createRequestResponseFrame(byteBufAllocator, true, metadata, data);
  }

  @Override
  public RequestResponseFrame createNonFragment(
      ByteBufAllocator byteBufAllocator, @Nullable ByteBuf metadata, @Nullable ByteBuf data) {

    return createRequestResponseFrame(byteBufAllocator, false, metadata, data);
  }

  @Override
  public ByteBuf getUnsafeData() {
    return getData(OFFSET_METADATA_LENGTH);
  }

  @Override
  public @Nullable ByteBuf getUnsafeMetadata() {
    return getMetadata(OFFSET_METADATA_LENGTH);
  }

  @Override
  public String toString() {
    return "RequestResponseFrame{"
        + "follows="
        + isFollowsFlagSet()
        + ", metadata="
        + mapMetadata(ByteBufUtil::hexDump)
        + ", data="
        + mapData(ByteBufUtil::hexDump)
        + '}';
  }
}
