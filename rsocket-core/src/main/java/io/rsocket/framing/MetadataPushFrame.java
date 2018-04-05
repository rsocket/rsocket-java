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

import static io.rsocket.framing.FrameType.METADATA_PUSH;
import static io.rsocket.util.RecyclerFactory.createRecycler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import java.util.Objects;
import reactor.util.annotation.Nullable;

/**
 * An RSocket {@code METADATA_PUSH} frame.
 *
 * @see <a
 *     href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#metadata_push-frame-0x0c">Metadata
 *     Push Frame</a>
 */
public final class MetadataPushFrame extends AbstractRecyclableMetadataFrame<MetadataPushFrame> {

  private static final int OFFSET_METADATA = FRAME_TYPE_AND_FLAGS_BYTES;

  private static final Recycler<MetadataPushFrame> RECYCLER =
      createRecycler(MetadataPushFrame::new);

  private MetadataPushFrame(Handle<MetadataPushFrame> handle) {
    super(handle);
  }

  /**
   * Creates the {@code METADATA_PUSH} frame.
   *
   * @param byteBuf the {@link ByteBuf} representing the frame
   * @return the {@code METADATA_PUSH} frame.
   * @throws NullPointerException if {@code byteBuf} is {@code null}
   */
  public static MetadataPushFrame createMetadataPushFrame(ByteBuf byteBuf) {
    Objects.requireNonNull(byteBuf, "byteBuf must not be null");

    return RECYCLER.get().setByteBuf(byteBuf.retain());
  }

  /**
   * Creates the {@code METADATA_PUSH} frame.
   *
   * @param byteBufAllocator the {@code ByteBufAllocator} to use
   * @param metadata the metadata
   * @return the {@code METADATA_PUSH} frame
   * @throws NullPointerException if {@code byteBufAllocator} or {@code metadata} is {@code null}
   */
  public static MetadataPushFrame createMetadataPushFrame(
      ByteBufAllocator byteBufAllocator, String metadata) {

    return createMetadataPushFrame(
        byteBufAllocator, getUtf8AsByteBufRequired(metadata, "metadata must not be null"));
  }

  /**
   * Creates the {@code METADATA_PUSH} frame.
   *
   * @param byteBufAllocator the {@code ByteBufAllocator} to use
   * @param metadata the metadata
   * @return the {@code METADATA_PUSH} frame
   * @throws NullPointerException if {@code byteBufAllocator} or {@code metadata} is {@code null}
   */
  public static MetadataPushFrame createMetadataPushFrame(
      ByteBufAllocator byteBufAllocator, ByteBuf metadata) {

    Objects.requireNonNull(metadata, "metadata must not be null");

    ByteBuf byteBuf = createFrameTypeAndFlags(byteBufAllocator, METADATA_PUSH);
    byteBuf = appendMetadata(byteBuf, metadata);

    return RECYCLER.get().setByteBuf(byteBuf);
  }

  @Override
  public @Nullable ByteBuf getUnsafeMetadata() {
    return getMetadata(OFFSET_METADATA);
  }

  @Override
  public String toString() {
    return "MetadataPushFrame{" + "metadata=" + mapMetadata(ByteBufUtil::hexDump) + '}';
  }
}
