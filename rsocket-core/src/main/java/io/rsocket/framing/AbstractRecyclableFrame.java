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
import static java.nio.charset.StandardCharsets.UTF_8;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.Recycler.Handle;
import io.netty.util.ReferenceCounted;
import java.util.Objects;
import reactor.util.annotation.Nullable;

/**
 * An abstract implementation of {@link Frame} that enables recycling for performance.
 *
 * @param <SELF> the implementing type
 * @see io.netty.util.Recycler
 */
abstract class AbstractRecyclableFrame<SELF extends AbstractRecyclableFrame<SELF>>
    implements Frame {

  /** The size of the {@link FrameType} and flags in {@code byte}s. */
  static final int FRAME_TYPE_AND_FLAGS_BYTES = 2;

  private static final int FLAGS_MASK = 0b00000011_11111111;

  private final Handle<SELF> handle;

  private ByteBuf byteBuf;

  AbstractRecyclableFrame(Handle<SELF> handle) {
    this.handle = handle;
  }

  @Override
  @SuppressWarnings("unchecked")
  public final void dispose() {
    if (byteBuf != null) {
      release(byteBuf);
    }

    byteBuf = null;
    handle.recycle((SELF) this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AbstractRecyclableFrame)) {
      return false;
    }
    AbstractRecyclableFrame<?> that = (AbstractRecyclableFrame<?>) o;
    return Objects.equals(byteBuf, that.byteBuf);
  }

  public FrameType getFrameType() {
    int encodedType = byteBuf.getUnsignedShort(0) >> FRAME_TYPE_SHIFT;
    return FrameType.fromEncodedType(encodedType);
  }

  public final ByteBuf getUnsafeFrame() {
    return byteBuf.asReadOnly();
  }

  @Override
  public int hashCode() {
    return Objects.hash(byteBuf);
  }

  /**
   * Create the {@link FrameType} and flags.
   *
   * @param byteBufAllocator the {@link ByteBufAllocator} to use
   * @param frameType the {@link FrameType}
   * @return the {@link ByteBuf} with {@link FrameType} and {@code flags} appended to it
   * @throws NullPointerException if {@code byteBuf} or {@code frameType} is {@code null}
   */
  static ByteBuf createFrameTypeAndFlags(ByteBufAllocator byteBufAllocator, FrameType frameType) {
    Objects.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    Objects.requireNonNull(frameType, "frameType must not be null");

    return byteBufAllocator.buffer().writeShort(getFrameTypeAndFlags(frameType));
  }

  /**
   * Returns the {@link String} as a {@code UTF-8} encoded {@link ByteBuf}.
   *
   * @param s the {@link String} to convert
   * @return the {@link String} as a {@code UTF-8} encoded {@link ByteBuf} or {@code null} if {@code
   *     s} is {@code null}.
   */
  static @Nullable ByteBuf getUtf8AsByteBuf(@Nullable String s) {
    return s == null ? null : Unpooled.copiedBuffer(s, UTF_8);
  }

  /**
   * Returns the {@link String} as a {@code UTF-8} encoded {@link ByteBuf}.
   *
   * @param s the {@link String} to convert
   * @return the {@link String} as a {@code UTF-8} encoded {@link ByteBuf}
   * @throws NullPointerException if {@code s} is {@code null}
   */
  static ByteBuf getUtf8AsByteBufRequired(String s, String message) {
    Objects.requireNonNull(s, message);
    return Unpooled.copiedBuffer(s, UTF_8);
  }

  /**
   * Sets a flag.
   *
   * @param byteBuf the {@link ByteBuf} to set the flag on
   * @param flag the flag to set
   * @return the {@link ByteBuf} with the flag set
   * @throws NullPointerException if {@code byteBuf} is {@code null}
   */
  static ByteBuf setFlag(ByteBuf byteBuf, int flag) {
    Objects.requireNonNull(byteBuf, "byteBuf must not be null");

    return byteBuf.setShort(0, byteBuf.getShort(0) | (flag & FLAGS_MASK));
  }

  /**
   * Returns the internal {@link ByteBuf}.
   *
   * @return the internal {@link ByteBuf}
   * @throws NullPointerException if {@code byteBuf} is {@code null}
   */
  final ByteBuf getByteBuf() {
    return Objects.requireNonNull(byteBuf, "byteBuf must not be null");
  }

  /**
   * Sets the internal {@link ByteBuf}.
   *
   * @param byteBuf the {@link ByteBuf}
   * @return {@code this}
   */
  @SuppressWarnings("unchecked")
  final SELF setByteBuf(ByteBuf byteBuf) {
    this.byteBuf = byteBuf;
    return (SELF) this;
  }

  /**
   * Returns whether a {@code flag} is set.
   *
   * @param flag the {@code flag} to test for
   * @return whether a {@code flag} is set
   * @throws NullPointerException if {@code byteBuf} is {@code null}
   */
  final boolean isFlagSet(int flag) {
    return (getByteBuf().getShort(0) & flag) != 0;
  }

  private static int getFrameTypeAndFlags(FrameType frameType) {
    return frameType.getEncodedType() << FRAME_TYPE_SHIFT;
  }
}
