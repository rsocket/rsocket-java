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
import static io.rsocket.framing.FrameType.EXT;
import static io.rsocket.util.RecyclerFactory.createRecycler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import java.util.Objects;
import reactor.util.annotation.Nullable;

/**
 * An RSocket {@code EXT} frame.
 *
 * @see <a
 *     href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#ext-extension-frame-0x3f9">Extension
 *     Frame</a>
 */
public final class ExtensionFrame extends AbstractRecyclableMetadataAndDataFrame<ExtensionFrame> {

  private static final int FLAG_IGNORE = 1 << 9;

  private static final int OFFSET_EXTENDED_TYPE = FRAME_TYPE_AND_FLAGS_BYTES;

  private static final int OFFSET_METADATA_LENGTH = OFFSET_EXTENDED_TYPE + Integer.BYTES;

  private static final Recycler<ExtensionFrame> RECYCLER = createRecycler(ExtensionFrame::new);

  private ExtensionFrame(Handle<ExtensionFrame> handle) {
    super(handle);
  }

  /**
   * Creates the {@code EXT} frame.
   *
   * @param byteBuf the {@link ByteBuf} representing the frame
   * @return the {@code EXT} frame.
   * @throws NullPointerException if {@code byteBuf} is {@code null}
   */
  public static ExtensionFrame createExtensionFrame(ByteBuf byteBuf) {
    Objects.requireNonNull(byteBuf, "byteBuf must not be null");

    return RECYCLER.get().setByteBuf(byteBuf.retain());
  }

  /**
   * Creates the {@code EXT} frame.
   *
   * @param byteBufAllocator the {@code ByteBufAllocator} to use
   * @param ignore whether to set the Ignore flag
   * @param extendedType the type of the extended frame
   * @param metadata the {@code metadata}
   * @param data the {@code data}
   * @return the {@code EXT} frame
   * @throws NullPointerException if {@code byteBufAllocator} is {@code null}
   */
  public static ExtensionFrame createExtensionFrame(
      ByteBufAllocator byteBufAllocator,
      boolean ignore,
      int extendedType,
      @Nullable String metadata,
      @Nullable String data) {

    ByteBuf metadataByteBuf = getUtf8AsByteBuf(metadata);
    ByteBuf dataByteBuf = getUtf8AsByteBuf(data);

    try {
      return createExtensionFrame(
          byteBufAllocator, ignore, extendedType, metadataByteBuf, dataByteBuf);
    } finally {
      release(metadataByteBuf);
      release(dataByteBuf);
    }
  }

  /**
   * Creates the {@code EXT} frame.
   *
   * @param byteBufAllocator the {@code ByteBufAllocator} to use
   * @param ignore whether to set the Ignore flag
   * @param extendedType the type of the extended frame
   * @param metadata the {@code metadata}
   * @param data the {@code data}
   * @return the {@code EXT} frame
   * @throws NullPointerException if {@code byteBufAllocator} is {@code null}
   */
  public static ExtensionFrame createExtensionFrame(
      ByteBufAllocator byteBufAllocator,
      boolean ignore,
      int extendedType,
      @Nullable ByteBuf metadata,
      @Nullable ByteBuf data) {

    ByteBuf byteBuf = createFrameTypeAndFlags(byteBufAllocator, EXT);

    if (ignore) {
      byteBuf = setFlag(byteBuf, FLAG_IGNORE);
    }

    byteBuf = byteBuf.writeInt(extendedType);
    byteBuf = appendMetadata(byteBufAllocator, byteBuf, metadata);
    byteBuf = appendData(byteBuf, data);

    return RECYCLER.get().setByteBuf(byteBuf);
  }

  /**
   * Returns the extended type.
   *
   * @return the extended type
   */
  public int getExtendedType() {
    return getByteBuf().getInt(OFFSET_EXTENDED_TYPE);
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
   * Returns whether the Ignore flag is set.
   *
   * @return whether the Ignore flag is set
   */
  public boolean isIgnoreFlagSet() {
    return isFlagSet(FLAG_IGNORE);
  }

  @Override
  public String toString() {
    return "ExtensionFrame{"
        + "ignore="
        + isIgnoreFlagSet()
        + ", extendedType="
        + getExtendedType()
        + ", metadata="
        + mapMetadata(ByteBufUtil::hexDump)
        + ", data="
        + mapData(ByteBufUtil::hexDump)
        + '}';
  }
}
