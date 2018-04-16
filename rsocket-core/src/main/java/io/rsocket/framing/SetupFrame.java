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
import static io.rsocket.framing.LengthUtils.getLengthAsUnsignedByte;
import static io.rsocket.framing.LengthUtils.getLengthAsUnsignedShort;
import static io.rsocket.util.NumberUtils.requireUnsignedShort;
import static io.rsocket.util.RecyclerFactory.createRecycler;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.rsocket.util.NumberUtils;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import reactor.util.annotation.Nullable;

/**
 * An RSocket {@code SETUP} frame.
 *
 * @see <a href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#setup-frame-0x01">Setup
 *     Frame</a>
 */
public final class SetupFrame extends AbstractRecyclableMetadataAndDataFrame<SetupFrame> {

  private static final int FLAG_LEASE = 1 << 6;

  private static final int FLAG_RESUME_ENABLED = 1 << 7;

  private static final int OFFSET_MAJOR_VERSION = FRAME_TYPE_AND_FLAGS_BYTES;

  private static final int OFFSET_MINOR_VERSION = OFFSET_MAJOR_VERSION + Short.BYTES;

  private static final int OFFSET_KEEPALIVE_INTERVAL = OFFSET_MINOR_VERSION + Short.BYTES;

  private static final int OFFSET_MAX_LIFETIME = OFFSET_KEEPALIVE_INTERVAL + Integer.BYTES;

  private static final int OFFSET_RESUME_IDENTIFICATION_TOKEN_LENGTH =
      OFFSET_MAX_LIFETIME + Integer.BYTES;

  private static final Recycler<SetupFrame> RECYCLER = createRecycler(SetupFrame::new);

  private SetupFrame(Handle<SetupFrame> handle) {
    super(handle);
  }

  /**
   * Creates the {@code SETUP} frame.
   *
   * @param byteBuf the {@link ByteBuf} representing the frame
   * @return the {@code SETUP} frame.
   * @throws NullPointerException if {@code byteBuf} is {@code null}
   */
  public static SetupFrame createSetupFrame(ByteBuf byteBuf) {
    Objects.requireNonNull(byteBuf, "byteBuf must not be null");

    return RECYCLER.get().setByteBuf(byteBuf.retain());
  }

  /**
   * Creates the {@code SETUP} frame.
   *
   * @param byteBufAllocator the {@code ByteBufAllocator} to use
   * @param lease whether to set the Lease flag
   * @param keepAliveInterval the time between {@code KEEPALIVE} frames
   * @param maxLifetime the time between {@code KEEPALIVE} frames before the server is assumed to be
   *     dead
   * @param resumeIdentificationToken the resume identification token
   * @param metadataMimeType metadata MIME-type encoding
   * @param dataMimeType data MIME-type encoding
   * @param metadata the {@code metadata}
   * @param data the {@code data}
   * @return the {@code SETUP} frame
   * @throws NullPointerException if {@code byteBufAllocator}, {@code keepAliveInterval}, {@code
   *     maxLifetime}, {@code metadataMimeType}, or {@code dataMimeType} is {@code null}
   * @throws IllegalArgumentException if {@code keepAliveInterval} or {@code maxLifetime} is not a
   *     positive duration
   */
  public static SetupFrame createSetupFrame(
      ByteBufAllocator byteBufAllocator,
      boolean lease,
      Duration keepAliveInterval,
      Duration maxLifetime,
      @Nullable String resumeIdentificationToken,
      String metadataMimeType,
      String dataMimeType,
      @Nullable String metadata,
      @Nullable String data) {

    ByteBuf resumeIdentificationTokenByteBuf = getUtf8AsByteBuf(resumeIdentificationToken);
    ByteBuf metadataByteBuf = getUtf8AsByteBuf(metadata);
    ByteBuf dataByteBuf = getUtf8AsByteBuf(data);

    try {
      return createSetupFrame(
          byteBufAllocator,
          lease,
          keepAliveInterval,
          maxLifetime,
          resumeIdentificationTokenByteBuf,
          metadataMimeType,
          dataMimeType,
          metadataByteBuf,
          dataByteBuf);
    } finally {
      release(resumeIdentificationTokenByteBuf);
      release(metadataByteBuf);
      release(dataByteBuf);
    }
  }

  /**
   * Creates the {@code SETUP} frame.
   *
   * @param byteBufAllocator the {@code ByteBufAllocator} to use
   * @param lease whether to set the Lease flag
   * @param keepAliveInterval the time between {@code KEEPALIVE} frames
   * @param maxLifetime the time between {@code KEEPALIVE} frames before the server is assumed to be
   *     dead
   * @param resumeIdentificationToken the resume identification token
   * @param metadataMimeType metadata MIME-type encoding
   * @param dataMimeType data MIME-type encoding
   * @param metadata the {@code metadata}
   * @param data the {@code data}
   * @return the {@code SETUP} frame
   * @throws NullPointerException if {@code byteBufAllocator}, {@code keepAliveInterval}, {@code
   *     maxLifetime}, {@code metadataMimeType}, or {@code dataMimeType} is {@code null}
   * @throws IllegalArgumentException if {@code keepAliveInterval} or {@code maxLifetime} is not a
   *     positive duration
   */
  public static SetupFrame createSetupFrame(
      ByteBufAllocator byteBufAllocator,
      boolean lease,
      Duration keepAliveInterval,
      Duration maxLifetime,
      @Nullable ByteBuf resumeIdentificationToken,
      String metadataMimeType,
      String dataMimeType,
      @Nullable ByteBuf metadata,
      @Nullable ByteBuf data) {

    return createSetupFrame(
        byteBufAllocator,
        lease,
        1,
        0,
        keepAliveInterval,
        maxLifetime,
        resumeIdentificationToken,
        metadataMimeType,
        dataMimeType,
        metadata,
        data);
  }

  /**
   * Creates the {@code SETUP} frame.
   *
   * @param byteBufAllocator the {@code ByteBufAllocator} to use
   * @param lease whether to set the Lease flag
   * @param majorVersion the major version of the protocol
   * @param minorVersion the minor version of the protocol
   * @param keepAliveInterval the time between {@code KEEPALIVE} frames
   * @param maxLifetime the time between {@code KEEPALIVE} frames before the server is assumed to be
   *     dead
   * @param resumeIdentificationToken the resume identification token
   * @param metadataMimeType metadata MIME-type encoding
   * @param dataMimeType data MIME-type encoding
   * @param metadata the {@code metadata}
   * @param data the {@code data}
   * @return the {@code SETUP} frame
   * @throws NullPointerException if {@code byteBufAllocator}, {@code keepAliveInterval}, {@code
   *     maxLifetime}, {@code metadataMimeType}, or {@code dataMimeType} is {@code null}
   * @throws IllegalArgumentException if {@code keepAliveInterval} or {@code maxLifetime} is not a
   *     positive duration
   */
  public static SetupFrame createSetupFrame(
      ByteBufAllocator byteBufAllocator,
      boolean lease,
      int majorVersion,
      int minorVersion,
      Duration keepAliveInterval,
      Duration maxLifetime,
      @Nullable ByteBuf resumeIdentificationToken,
      String metadataMimeType,
      String dataMimeType,
      @Nullable ByteBuf metadata,
      @Nullable ByteBuf data) {

    Objects.requireNonNull(keepAliveInterval, "keepAliveInterval must not be null");
    NumberUtils.requirePositive(
        keepAliveInterval.toMillis(), "keepAliveInterval must be a positive duration");
    Objects.requireNonNull(maxLifetime, "maxLifetime must not be null");
    NumberUtils.requirePositive(maxLifetime.toMillis(), "maxLifetime must be a positive duration");

    ByteBuf metadataMimeTypeByteBuf =
        getUtf8AsByteBufRequired(metadataMimeType, "metadataMimeType must not be null");
    ByteBuf dataMimeTypeByteBuf =
        getUtf8AsByteBufRequired(dataMimeType, "dataMimeType must not be null");

    try {
      ByteBuf byteBuf = createFrameTypeAndFlags(byteBufAllocator, FrameType.SETUP);

      if (lease) {
        byteBuf = setFlag(byteBuf, FLAG_LEASE);
      }

      byteBuf =
          byteBuf
              .writeShort(requireUnsignedShort(majorVersion))
              .writeShort(requireUnsignedShort(minorVersion))
              .writeInt(toIntExact(keepAliveInterval.toMillis()))
              .writeInt(toIntExact(maxLifetime.toMillis()));

      if (resumeIdentificationToken != null) {
        byteBuf =
            setFlag(byteBuf, FLAG_RESUME_ENABLED)
                .writeShort(getLengthAsUnsignedShort(resumeIdentificationToken));
        byteBuf =
            Unpooled.wrappedBuffer(
                byteBuf, resumeIdentificationToken.retain(), byteBufAllocator.buffer());
      }

      byteBuf = byteBuf.writeByte(getLengthAsUnsignedByte(metadataMimeTypeByteBuf));
      byteBuf =
          Unpooled.wrappedBuffer(
              byteBuf, metadataMimeTypeByteBuf.retain(), byteBufAllocator.buffer());

      byteBuf = byteBuf.writeByte(getLengthAsUnsignedByte(dataMimeTypeByteBuf));
      byteBuf =
          Unpooled.wrappedBuffer(byteBuf, dataMimeTypeByteBuf.retain(), byteBufAllocator.buffer());

      byteBuf = appendMetadata(byteBufAllocator, byteBuf, metadata);
      byteBuf = appendData(byteBuf, data);

      return RECYCLER.get().setByteBuf(byteBuf);
    } finally {
      release(metadataMimeTypeByteBuf);
      release(dataMimeTypeByteBuf);
    }
  }

  /**
   * Returns the data MIME-type, decoded at {@link StandardCharsets#UTF_8}.
   *
   * @return the data MIME-type, decoded as {@link StandardCharsets#UTF_8}
   */
  public String getDataMimeType() {
    return getDataMimeType(UTF_8);
  }

  /**
   * Returns the data MIME-type.
   *
   * @param charset the {@link Charset} to decode the data MIME-type with
   * @return the data MIME-type
   */
  public String getDataMimeType(Charset charset) {
    return getByteBuf().slice(getDataMimeTypeOffset(), getDataMimeTypeLength()).toString(charset);
  }

  /**
   * Returns the keep alive interval.
   *
   * @return the keep alive interval
   */
  public Duration getKeepAliveInterval() {
    return Duration.ofMillis(getByteBuf().getInt(OFFSET_KEEPALIVE_INTERVAL));
  }

  /**
   * Returns the major version of the protocol.
   *
   * @return the major version of the protocol
   */
  public int getMajorVersion() {
    return getByteBuf().getUnsignedShort(OFFSET_MAJOR_VERSION);
  }

  /**
   * Returns the max lifetime.
   *
   * @return the max lifetime
   */
  public Duration getMaxLifetime() {
    return Duration.ofMillis(getByteBuf().getInt(OFFSET_MAX_LIFETIME));
  }

  /**
   * Returns the metadata MIME-type, decoded at {@link StandardCharsets#UTF_8}.
   *
   * @return the metadata MIME-type, decoded as {@link StandardCharsets#UTF_8}
   */
  public String getMetadataMimeType() {
    return getMetadataMimeType(UTF_8);
  }

  /**
   * Returns the metadata MIME-type.
   *
   * @param charset the {@link Charset} to decode the metadata MIME-type with
   * @return the metadata MIME-type
   */
  public String getMetadataMimeType(Charset charset) {
    return getByteBuf()
        .slice(getMetadataMimeTypeOffset(), getMetadataMimeTypeLength())
        .toString(charset);
  }

  /**
   * Returns the minor version of the protocol.
   *
   * @return the minor version of the protocol
   */
  public int getMinorVersion() {
    return getByteBuf().getUnsignedShort(OFFSET_MINOR_VERSION);
  }

  /**
   * Returns the resume identification token as a UTF-8 {@link String}. If the Resume Enabled flag
   * is not set, returns {@link Optional#empty()}.
   *
   * @return optionally, the resume identification token as a UTF-8 {@link String}
   */
  public Optional<String> getResumeIdentificationTokenAsUtf8() {
    return Optional.ofNullable(getUnsafeResumeIdentificationTokenAsUtf8());
  }

  @Override
  public ByteBuf getUnsafeData() {
    return getData(getMetadataLengthOffset());
  }

  @Override
  public @Nullable ByteBuf getUnsafeMetadata() {
    return getMetadata(getMetadataLengthOffset());
  }

  /**
   * Returns the resume identification token directly. If the Resume Enabled flag is not set,
   * returns {@code null}.
   *
   * <p><b>Note:</b> this resume identification token will be outside of the {@link Frame}'s
   * lifecycle and may be released at any time. It is highly recommended that you {@link
   * ByteBuf#retain()} the resume identification token if you store it.
   *
   * @return the resume identification token directly, or {@code null} if the Resume Enabled flag is
   *     not set
   * @see #mapResumeIdentificationToken(Function)
   */
  public @Nullable ByteBuf getUnsafeResumeIdentificationToken() {
    if (!isFlagSet(FLAG_RESUME_ENABLED)) {
      return null;
    }

    ByteBuf byteBuf = getByteBuf();
    return byteBuf.slice(
        getResumeIdentificationTokenOffset(), getResumeIdentificationTokenLength());
  }

  /**
   * Returns the resume identification token as a UTF-8 {@link String}. If the Resume Enabled flag
   * is not set, returns {@code null}.
   *
   * @return the resume identification token as a UTF-8 {@link String} or {@code null} if the Resume
   *     Enabled flag is not set.
   * @see #getResumeIdentificationTokenAsUtf8()
   */
  public @Nullable String getUnsafeResumeIdentificationTokenAsUtf8() {
    ByteBuf byteBuf = getUnsafeResumeIdentificationToken();
    return byteBuf == null ? null : byteBuf.toString(UTF_8);
  }

  /**
   * Returns whether the lease flag is set.
   *
   * @return whether the lease flag is set
   */
  public boolean isLeaseFlagSet() {
    return isFlagSet(FLAG_LEASE);
  }

  /**
   * Exposes the resume identification token for mapping to a different type. If the Resume Enabled
   * flag is not set, returns {@link Optional#empty()}.
   *
   * @param function the function to transform the resume identification token to a different type
   * @param <T> the different type
   * @return optionally, the resume identification token mapped to a different type
   * @throws NullPointerException if {@code function} is {@code null}
   */
  public <T> Optional<T> mapResumeIdentificationToken(Function<ByteBuf, T> function) {
    Objects.requireNonNull(function, "function must not be null");

    return Optional.ofNullable(getUnsafeResumeIdentificationToken()).map(function);
  }

  @Override
  public String toString() {
    return "SetupFrame{"
        + "lease="
        + isLeaseFlagSet()
        + ", majorVersion="
        + getMajorVersion()
        + ", minorVersion="
        + getMinorVersion()
        + ", keepAliveInterval="
        + getKeepAliveInterval()
        + ", maxLifetime="
        + getMaxLifetime()
        + ", resumeIdentificationToken="
        + mapResumeIdentificationToken(ByteBufUtil::hexDump)
        + ", metadataMimeType="
        + getMetadataMimeType()
        + ", dataMimeType="
        + getDataMimeType()
        + ", metadata="
        + mapMetadata(ByteBufUtil::hexDump)
        + ", data="
        + mapData(ByteBufUtil::hexDump)
        + '}';
  }

  private int getDataMimeTypeLength() {
    return getByteBuf().getUnsignedByte(getDataMimeTypeLengthOffset());
  }

  private int getDataMimeTypeLengthOffset() {
    return getMetadataMimeTypeOffset() + getMetadataMimeTypeLength();
  }

  private int getDataMimeTypeOffset() {
    return getDataMimeTypeLengthOffset() + Byte.BYTES;
  }

  private int getMetadataLengthOffset() {
    return getDataMimeTypeOffset() + getDataMimeTypeLength();
  }

  private int getMetadataMimeTypeLength() {
    return getByteBuf().getUnsignedByte(getMetadataMimeTypeLengthOffset());
  }

  private int getMetadataMimeTypeLengthOffset() {
    return getResumeIdentificationTokenOffset() + getResumeIdentificationTokenLength();
  }

  private int getMetadataMimeTypeOffset() {
    return getMetadataMimeTypeLengthOffset() + Byte.BYTES;
  }

  private int getResumeIdentificationTokenLength() {
    if (isFlagSet(FLAG_RESUME_ENABLED)) {
      return getByteBuf().getUnsignedShort(OFFSET_RESUME_IDENTIFICATION_TOKEN_LENGTH);
    } else {
      return 0;
    }
  }

  private int getResumeIdentificationTokenOffset() {
    if (isFlagSet(FLAG_RESUME_ENABLED)) {
      return OFFSET_RESUME_IDENTIFICATION_TOKEN_LENGTH + Short.BYTES;
    } else {
      return OFFSET_RESUME_IDENTIFICATION_TOKEN_LENGTH;
    }
  }
}
