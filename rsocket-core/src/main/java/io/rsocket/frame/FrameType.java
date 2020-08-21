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

package io.rsocket.frame;

import java.util.Arrays;

/**
 * Types of Frame that can be sent.
 *
 * @see <a href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-types">Frame
 *     Types</a>
 */
public enum FrameType {

  /** Reserved. */
  RESERVED(0x00),

  // CONNECTION

  /**
   * Sent by client to initiate protocol processing.
   *
   * @see <a
   *     href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#setup-frame-0x01">Setup
   *     Frame</a>
   */
  SETUP(0x01, Flags.CAN_HAVE_DATA | Flags.CAN_HAVE_METADATA),

  /**
   * Sent by Responder to grant the ability to send requests.
   *
   * @see <a
   *     href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#lease-frame-0x02">Lease
   *     Frame</a>
   */
  LEASE(0x02, Flags.CAN_HAVE_METADATA),

  /**
   * Connection keepalive.
   *
   * @see <a
   *     href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-keepalive">Keepalive
   *     Frame</a>
   */
  KEEPALIVE(0x03, Flags.CAN_HAVE_DATA),

  // START REQUEST

  /**
   * Request single response.
   *
   * @see <a
   *     href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-request-response">Request
   *     Response Frame</a>
   */
  REQUEST_RESPONSE(
      0x04,
      Flags.CAN_HAVE_DATA
          | Flags.CAN_HAVE_METADATA
          | Flags.IS_FRAGMENTABLE
          | Flags.IS_REQUEST_TYPE),

  /**
   * A single one-way message.
   *
   * @see <a href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-fnf">Request
   *     Fire-and-Forget Frame</a>
   */
  REQUEST_FNF(
      0x05,
      Flags.CAN_HAVE_DATA
          | Flags.CAN_HAVE_METADATA
          | Flags.IS_FRAGMENTABLE
          | Flags.IS_REQUEST_TYPE),

  /**
   * Request a completable stream.
   *
   * @see <a
   *     href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-request-stream">Request
   *     Stream Frame</a>
   */
  REQUEST_STREAM(
      0x06,
      Flags.CAN_HAVE_METADATA
          | Flags.CAN_HAVE_DATA
          | Flags.HAS_INITIAL_REQUEST_N
          | Flags.IS_FRAGMENTABLE
          | Flags.IS_REQUEST_TYPE),

  /**
   * Request a completable stream in both directions.
   *
   * @see <a
   *     href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-request-channel">Request
   *     Channel Frame</a>
   */
  REQUEST_CHANNEL(
      0x07,
      Flags.CAN_HAVE_METADATA
          | Flags.CAN_HAVE_DATA
          | Flags.HAS_INITIAL_REQUEST_N
          | Flags.IS_FRAGMENTABLE
          | Flags.IS_REQUEST_TYPE),

  // DURING REQUEST

  /**
   * Request N more items with Reactive Streams semantics.
   *
   * @see <a
   *     href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-request-n">RequestN
   *     Frame</a>
   */
  REQUEST_N(0x08),

  /**
   * Cancel outstanding request.
   *
   * @see <a href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-cancel">Cancel
   *     Frame</a>
   */
  CANCEL(0x09),

  // RESPONSE

  /**
   * Payload on a stream. For example, response to a request, or message on a channel.
   *
   * @see <a href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-payload">Payload
   *     Frame</a>
   */
  PAYLOAD(0x0A, Flags.CAN_HAVE_DATA | Flags.CAN_HAVE_METADATA | Flags.IS_FRAGMENTABLE),

  /**
   * Error at connection or application level.
   *
   * @see <a href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-error">Error
   *     Frame</a>
   */
  ERROR(0x0B, Flags.CAN_HAVE_DATA),

  // METADATA

  /**
   * Asynchronous Metadata frame.
   *
   * @see <a
   *     href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-metadata-push">Metadata
   *     Push Frame</a>
   */
  METADATA_PUSH(0x0C, Flags.CAN_HAVE_METADATA),

  // RESUMPTION

  /**
   * Replaces SETUP for Resuming Operation (optional).
   *
   * @see <a href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-resume">Resume
   *     Frame</a>
   */
  RESUME(0x0D),

  /**
   * Sent in response to a RESUME if resuming operation possible (optional).
   *
   * @see <a
   *     href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-resume-ok">Resume OK
   *     Frame</a>
   */
  RESUME_OK(0x0E),

  // SYNTHETIC PAYLOAD TYPES

  /** A {@link #PAYLOAD} frame with {@code NEXT} flag set. */
  NEXT(0xA0, Flags.CAN_HAVE_DATA | Flags.CAN_HAVE_METADATA | Flags.IS_FRAGMENTABLE),

  /** A {@link #PAYLOAD} frame with {@code COMPLETE} flag set. */
  COMPLETE(0xB0),

  /** A {@link #PAYLOAD} frame with {@code NEXT} and {@code COMPLETE} flags set. */
  NEXT_COMPLETE(0xC0, Flags.CAN_HAVE_DATA | Flags.CAN_HAVE_METADATA | Flags.IS_FRAGMENTABLE),

  // SYNTHETIC REQUEST_CHANNEL WITH COMPLETION

  /** A {@link #REQUEST_CHANNEL} and {@code COMPLETE} flags set. */
  REQUEST_CHANNEL_COMPLETE(
      0xD7,
      Flags.CAN_HAVE_METADATA
          | Flags.CAN_HAVE_DATA
          | Flags.HAS_INITIAL_REQUEST_N
          | Flags.IS_FRAGMENTABLE
          | Flags.IS_REQUEST_TYPE),

  /**
   * Used To Extend more frame types as well as extensions.
   *
   * @see <a href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-ext">Extension
   *     Frame</a>
   */
  EXT(0x3F, Flags.CAN_HAVE_DATA | Flags.CAN_HAVE_METADATA);

  /** The size of the encoded frame type */
  static final int ENCODED_SIZE = 6;

  private static final FrameType[] FRAME_TYPES_BY_ENCODED_TYPE;

  static {
    FRAME_TYPES_BY_ENCODED_TYPE = new FrameType[getMaximumEncodedType() + 1];

    for (FrameType frameType : values()) {
      FRAME_TYPES_BY_ENCODED_TYPE[frameType.encodedType] = frameType;
    }
  }

  private final int encodedType;
  private final int flags;

  FrameType(int encodedType) {
    this(encodedType, Flags.EMPTY);
  }

  FrameType(int encodedType, int flags) {
    this.encodedType = encodedType;
    this.flags = flags;
  }

  /**
   * Returns the {@code FrameType} that matches the specified {@code encodedType}.
   *
   * @param encodedType the encoded type
   * @return the {@code FrameType} that matches the specified {@code encodedType}
   */
  public static FrameType fromEncodedType(int encodedType) {
    FrameType frameType = FRAME_TYPES_BY_ENCODED_TYPE[encodedType];

    if (frameType == null) {
      throw new IllegalArgumentException(String.format("Frame type %d is unknown", encodedType));
    }

    return frameType;
  }

  private static int getMaximumEncodedType() {
    return Arrays.stream(values()).mapToInt(frameType -> frameType.encodedType).max().orElse(0);
  }

  /**
   * Whether the frame type can have data.
   *
   * @return whether the frame type can have data
   */
  public boolean canHaveData() {
    return Flags.CAN_HAVE_DATA == (flags & Flags.CAN_HAVE_DATA);
  }

  /**
   * Whether the frame type can have metadata
   *
   * @return whether the frame type can have metadata
   */
  public boolean canHaveMetadata() {
    return Flags.CAN_HAVE_METADATA == (flags & Flags.CAN_HAVE_METADATA);
  }

  /**
   * Returns the encoded type.
   *
   * @return the encoded type
   */
  public int getEncodedType() {
    return encodedType;
  }

  /**
   * Whether the frame type starts with an initial {@code requestN}.
   *
   * @return wether the frame type starts with an initial {@code requestN}
   */
  public boolean hasInitialRequestN() {
    return Flags.HAS_INITIAL_REQUEST_N == (flags & Flags.HAS_INITIAL_REQUEST_N);
  }

  /**
   * Whether the frame type is fragmentable.
   *
   * @return whether the frame type is fragmentable
   */
  public boolean isFragmentable() {
    return Flags.IS_FRAGMENTABLE == (flags & Flags.IS_FRAGMENTABLE);
  }

  /**
   * Whether the frame type is a request type.
   *
   * @return whether the frame type is a request type
   */
  public boolean isRequestType() {
    return Flags.IS_REQUEST_TYPE == (flags & Flags.IS_REQUEST_TYPE);
  }

  private static class Flags {
    private static final int EMPTY = 0b00000;
    private static final int CAN_HAVE_DATA = 0b10000;
    private static final int CAN_HAVE_METADATA = 0b01000;
    private static final int IS_FRAGMENTABLE = 0b00100;
    private static final int IS_REQUEST_TYPE = 0b00010;
    private static final int HAS_INITIAL_REQUEST_N = 0b00001;

    private Flags() {}
  }
}
