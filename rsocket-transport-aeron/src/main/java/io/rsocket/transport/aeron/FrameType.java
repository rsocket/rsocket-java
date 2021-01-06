/*
 * Copyright 2015-present the original author or authors.
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

package io.rsocket.transport.aeron;

import java.util.Arrays;

/**
 * Types of frame that can be sent.
 *
 * @see <a href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-types">Frame
 *     Types</a>
 */
enum FrameType {

  /** Reserved. */
  RESERVED(0x00),

  SETUP(0x01),

  SETUP_COMPLETE(0x02);

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

  FrameType(int encodedType) {
    this.encodedType = encodedType;
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

  public int getEncodedType() {
    return encodedType;
  }
}
