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

import static io.rsocket.framing.FrameType.RESUME;
import static io.rsocket.framing.LengthUtils.getLengthAsUnsignedShort;
import static io.rsocket.util.NumberUtils.requireUnsignedShort;
import static io.rsocket.util.RecyclerFactory.createRecycler;
import static java.nio.charset.StandardCharsets.UTF_8;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import java.util.Objects;
import java.util.function.Function;

/**
 * An RSocket {@code RESUME} frame.
 *
 * @see <a href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-resume">Resume
 *     Frame</a>
 */
public final class ResumeFrame extends AbstractRecyclableFrame<ResumeFrame> {

  private static final int OFFSET_MAJOR_VERSION = FRAME_TYPE_AND_FLAGS_BYTES;

  private static final int OFFSET_MINOR_VERSION = OFFSET_MAJOR_VERSION + Short.BYTES;

  private static final int OFFSET_TOKEN_LENGTH = OFFSET_MINOR_VERSION + Short.BYTES;

  private static final int OFFSET_RESUME_IDENTIFICATION_TOKEN = OFFSET_TOKEN_LENGTH + Short.BYTES;

  private static final Recycler<ResumeFrame> RECYCLER = createRecycler(ResumeFrame::new);

  private ResumeFrame(Handle<ResumeFrame> handle) {
    super(handle);
  }

  /**
   * Creates the {@code RESUME} frame.
   *
   * @param byteBufAllocator the {@code ByteBufAllocator} to use
   * @param resumeIdentificationToken the resume identification token
   * @param lastReceivedServerPosition the last received server position
   * @param firstAvailableClientPosition the first available client position
   * @return the {@code RESUME} frame
   * @throws NullPointerException if {@code byteBufAllocator} or {@code resumeIdentificationToken}
   *     is {@code null}
   */
  public static ResumeFrame createResumeFrame(
      ByteBufAllocator byteBufAllocator,
      String resumeIdentificationToken,
      long lastReceivedServerPosition,
      long firstAvailableClientPosition) {

    return createResumeFrame(
        byteBufAllocator,
        getUtf8AsByteBufRequired(
            resumeIdentificationToken, "resumeIdentificationToken must not be null"),
        lastReceivedServerPosition,
        firstAvailableClientPosition);
  }

  /**
   * Creates the {@code RESUME} frame.
   *
   * @param byteBufAllocator the {@code ByteBufAllocator} to use
   * @param resumeIdentificationToken the resume identification token
   * @param lastReceivedServerPosition the last received server position
   * @param firstAvailableClientPosition the first available client position
   * @return the {@code RESUME} frame
   * @throws NullPointerException if {@code byteBufAllocator} or {@code resumeIdentificationToken}
   *     is {@code null}
   */
  public static ResumeFrame createResumeFrame(
      ByteBufAllocator byteBufAllocator,
      ByteBuf resumeIdentificationToken,
      long lastReceivedServerPosition,
      long firstAvailableClientPosition) {

    return createResumeFrame(
        byteBufAllocator,
        1,
        0,
        resumeIdentificationToken,
        lastReceivedServerPosition,
        firstAvailableClientPosition);
  }

  /**
   * Creates the {@code RESUME} frame.
   *
   * @param byteBuf the {@link ByteBuf} representing the frame
   * @return the {@code RESUME} frame.
   * @throws NullPointerException if {@code byteBuf} is {@code null}
   */
  public static ResumeFrame createResumeFrame(ByteBuf byteBuf) {
    Objects.requireNonNull(byteBuf, "byteBuf must not be null");

    return RECYCLER.get().setByteBuf(byteBuf.retain());
  }

  /**
   * Creates the {@code RESUME} frame.
   *
   * @param byteBufAllocator the {@code ByteBufAllocator} to use
   * @param majorVersion the major version of the protocol
   * @param minorVersion the minor version of the protocol
   * @param resumeIdentificationToken the resume identification token
   * @param lastReceivedServerPosition the last received server position
   * @param firstAvailableClientPosition the first available client position
   * @return the {@code RESUME} frame
   * @throws NullPointerException if {@code byteBufAllocator} or {@code resumeIdentificationToken}
   *     is {@code null}
   */
  public static ResumeFrame createResumeFrame(
      ByteBufAllocator byteBufAllocator,
      int majorVersion,
      int minorVersion,
      ByteBuf resumeIdentificationToken,
      long lastReceivedServerPosition,
      long firstAvailableClientPosition) {

    Objects.requireNonNull(resumeIdentificationToken, "resumeIdentificationToken must not be null");

    ByteBuf byteBuf =
        createFrameTypeAndFlags(byteBufAllocator, RESUME)
            .writeShort(requireUnsignedShort(majorVersion))
            .writeShort(requireUnsignedShort(minorVersion));

    byteBuf = byteBuf.writeShort(getLengthAsUnsignedShort(resumeIdentificationToken));
    byteBuf =
        Unpooled.wrappedBuffer(
            byteBuf, resumeIdentificationToken.retain(), byteBufAllocator.buffer());

    byteBuf = byteBuf.writeLong(lastReceivedServerPosition).writeLong(firstAvailableClientPosition);

    return RECYCLER.get().setByteBuf(byteBuf);
  }

  /**
   * Returns the first available client position.
   *
   * @return the first available client position
   */
  public long getFirstAvailableClientPosition() {
    return getByteBuf().getLong(getFirstAvailableClientPositionOffset());
  }

  /**
   * Returns the last received server position.
   *
   * @return the last received server position
   */
  public long getLastReceivedServerPosition() {
    return getByteBuf().getLong(getLastReceivedServerPositionOffset());
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
   * Returns the minor version of the protocol.
   *
   * @return the minor version of the protocol
   */
  public int getMinorVersion() {
    return getByteBuf().getUnsignedShort(OFFSET_MINOR_VERSION);
  }

  /**
   * Returns the resume identification token as a UTF-8 {@link String}.
   *
   * @return the resume identification token as a UTF-8 {@link String}
   */
  public String getResumeIdentificationTokenAsUtf8() {
    return mapResumeIdentificationToken(byteBuf -> byteBuf.toString(UTF_8));
  }

  /**
   * Returns the resume identification token directly.
   *
   * <p><b>Note:</b> this resume identification token will be outside of the {@link Frame}'s
   * lifecycle and may be released at any time. It is highly recommended that you {@link
   * ByteBuf#retain()} the resume identification token if you store it.
   *
   * @return the resume identification token directly
   * @see #mapResumeIdentificationToken(Function)
   */
  public ByteBuf getUnsafeResumeIdentificationToken() {
    return getByteBuf().slice(OFFSET_RESUME_IDENTIFICATION_TOKEN, getTokenLength());
  }

  /**
   * Exposes the resume identification token for mapping to a different type.
   *
   * @param function the function to transform the resume identification token to a different type
   * @param <T> the different type
   * @return the resume identification token mapped to a different type
   * @throws NullPointerException if {@code function} is {@code null}
   */
  public <T> T mapResumeIdentificationToken(Function<ByteBuf, T> function) {
    Objects.requireNonNull(function, "function must not be null");

    return function.apply(getUnsafeResumeIdentificationToken());
  }

  @Override
  public String toString() {
    return "ResumeFrame{"
        + "majorVersion="
        + getMajorVersion()
        + ", minorVersion="
        + getMinorVersion()
        + ", resumeIdentificationToken="
        + mapResumeIdentificationToken(ByteBufUtil::hexDump)
        + ", lastReceivedServerPosition="
        + getLastReceivedServerPosition()
        + ", firstAvailableClientPosition="
        + getFirstAvailableClientPosition()
        + '}';
  }

  private int getFirstAvailableClientPositionOffset() {
    return getLastReceivedServerPositionOffset() + Long.BYTES;
  }

  private int getLastReceivedServerPositionOffset() {
    return OFFSET_RESUME_IDENTIFICATION_TOKEN + getTokenLength();
  }

  private int getTokenLength() {
    return getByteBuf().getUnsignedShort(OFFSET_TOKEN_LENGTH);
  }
}
