package io.rsocket.core;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.IllegalReferenceCountException;
import io.rsocket.Payload;
import reactor.util.annotation.Nullable;

public class UnpooledByteBufPayload extends AbstractReferenceCounted implements Payload {

  private final ByteBuf data;
  private final ByteBuf metadata;

  /**
   * Static factory method for a text payload. Mainly looks better than "new ByteBufPayload(data)"
   *
   * @param data the data of the payload.
   * @return a payload.
   */
  public static Payload create(String data) {
    return create(data, ByteBufAllocator.DEFAULT);
  }

  /**
   * Static factory method for a text payload. Mainly looks better than "new ByteBufPayload(data)"
   *
   * @param data the data of the payload.
   * @return a payload.
   */
  public static Payload create(String data, ByteBufAllocator allocator) {
    return new UnpooledByteBufPayload(ByteBufUtil.writeUtf8(allocator, data), null);
  }

  /**
   * Static factory method for a text payload. Mainly looks better than "new ByteBufPayload(data,
   * metadata)"
   *
   * @param data the data of the payload.
   * @param metadata the metadata for the payload.
   * @return a payload.
   */
  public static Payload create(String data, @Nullable String metadata) {
    return create(data, metadata, ByteBufAllocator.DEFAULT);
  }

  /**
   * Static factory method for a text payload. Mainly looks better than "new ByteBufPayload(data,
   * metadata)"
   *
   * @param data the data of the payload.
   * @param metadata the metadata for the payload.
   * @return a payload.
   */
  public static Payload create(String data, @Nullable String metadata, ByteBufAllocator allocator) {
    return new UnpooledByteBufPayload(
        ByteBufUtil.writeUtf8(allocator, data),
        metadata == null ? null : ByteBufUtil.writeUtf8(allocator, metadata));
  }

  public UnpooledByteBufPayload(ByteBuf data, @Nullable ByteBuf metadata) {
    this.data = data;
    this.metadata = metadata;
  }

  @Override
  public boolean hasMetadata() {
    ensureAccessible();
    return metadata != null;
  }

  @Override
  public ByteBuf sliceMetadata() {
    ensureAccessible();
    return metadata == null ? Unpooled.EMPTY_BUFFER : metadata.slice();
  }

  @Override
  public ByteBuf data() {
    ensureAccessible();
    return data;
  }

  @Override
  public ByteBuf metadata() {
    ensureAccessible();
    return metadata == null ? Unpooled.EMPTY_BUFFER : metadata;
  }

  @Override
  public ByteBuf sliceData() {
    ensureAccessible();
    return data.slice();
  }

  @Override
  public UnpooledByteBufPayload retain() {
    super.retain();
    return this;
  }

  @Override
  public UnpooledByteBufPayload retain(int increment) {
    super.retain(increment);
    return this;
  }

  @Override
  public UnpooledByteBufPayload touch() {
    ensureAccessible();
    data.touch();
    if (metadata != null) {
      metadata.touch();
    }
    return this;
  }

  @Override
  public UnpooledByteBufPayload touch(Object hint) {
    ensureAccessible();
    data.touch(hint);
    if (metadata != null) {
      metadata.touch(hint);
    }
    return this;
  }

  @Override
  protected void deallocate() {
    data.release();
    if (metadata != null) {
      metadata.release();
    }
  }

  /**
   * Should be called by every method that tries to access the buffers content to check if the
   * buffer was released before.
   */
  void ensureAccessible() {
    if (!isAccessible()) {
      throw new IllegalReferenceCountException(0);
    }
  }

  /**
   * Used internally by {@link UnpooledByteBufPayload#ensureAccessible()} to try to guard against
   * using the buffer after it was released (best-effort).
   */
  boolean isAccessible() {
    return refCnt() != 0;
  }
}
