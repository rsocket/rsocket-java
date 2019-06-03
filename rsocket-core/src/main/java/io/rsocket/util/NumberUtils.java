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

package io.rsocket.util;

import io.netty.buffer.ByteBuf;
import java.util.Objects;

public final class NumberUtils {

  /** The size of a medium in {@code byte}s. */
  public static final int MEDIUM_BYTES = 3;

  private static final int UNSIGNED_BYTE_SIZE = 8;

  private static final int UNSIGNED_BYTE_MAX_VALUE = (1 << UNSIGNED_BYTE_SIZE) - 1;

  private static final int UNSIGNED_MEDIUM_SIZE = 24;

  private static final int UNSIGNED_MEDIUM_MAX_VALUE = (1 << UNSIGNED_MEDIUM_SIZE) - 1;

  private static final int UNSIGNED_SHORT_SIZE = 16;

  private static final int UNSIGNED_SHORT_MAX_VALUE = (1 << UNSIGNED_SHORT_SIZE) - 1;

  private NumberUtils() {}

  /**
   * Requires that an {@code int} is greater than or equal to zero.
   *
   * @param i the {@code int} to test
   * @param message detail message to be used in the event that a {@link IllegalArgumentException}
   *     is thrown
   * @return the {@code int} if greater than or equal to zero
   * @throws IllegalArgumentException if {@code i} is less than zero
   */
  public static int requireNonNegative(int i, String message) {
    Objects.requireNonNull(message, "message must not be null");

    if (i < 0) {
      throw new IllegalArgumentException(message);
    }

    return i;
  }

  /**
   * Requires that a {@code long} is greater than zero.
   *
   * @param l the {@code long} to test
   * @param message detail message to be used in the event that a {@link IllegalArgumentException}
   *     is thrown
   * @return the {@code long} if greater than zero
   * @throws IllegalArgumentException if {@code l} is less than or equal to zero
   */
  public static long requirePositive(long l, String message) {
    Objects.requireNonNull(message, "message must not be null");

    if (l <= 0) {
      throw new IllegalArgumentException(message);
    }

    return l;
  }

  /**
   * Requires that an {@code int} is greater than zero.
   *
   * @param i the {@code int} to test
   * @param message detail message to be used in the event that a {@link IllegalArgumentException}
   *     is thrown
   * @return the {@code int} if greater than zero
   * @throws IllegalArgumentException if {@code i} is less than or equal to zero
   */
  public static int requirePositive(int i, String message) {
    Objects.requireNonNull(message, "message must not be null");

    if (i <= 0) {
      throw new IllegalArgumentException(message);
    }

    return i;
  }

  /**
   * Requires that an {@code int} can be represented as an unsigned {@code byte}.
   *
   * @param i the {@code int} to test
   * @return the {@code int} if it can be represented as an unsigned {@code byte}
   * @throws IllegalArgumentException if {@code i} cannot be represented as an unsigned {@code byte}
   */
  public static int requireUnsignedByte(int i) {
    if (i > UNSIGNED_BYTE_MAX_VALUE) {
      throw new IllegalArgumentException(
          String.format("%d is larger than %d bits", i, UNSIGNED_BYTE_SIZE));
    }

    return i;
  }

  /**
   * Requires that an {@code int} can be represented as an unsigned {@code medium}.
   *
   * @param i the {@code int} to test
   * @return the {@code int} if it can be represented as an unsigned {@code medium}
   * @throws IllegalArgumentException if {@code i} cannot be represented as an unsigned {@code
   *     medium}
   */
  public static int requireUnsignedMedium(int i) {
    if (i > UNSIGNED_MEDIUM_MAX_VALUE) {
      throw new IllegalArgumentException(
          String.format("%d is larger than %d bits", i, UNSIGNED_MEDIUM_SIZE));
    }

    return i;
  }

  /**
   * Requires that an {@code int} can be represented as an unsigned {@code short}.
   *
   * @param i the {@code int} to test
   * @return the {@code int} if it can be represented as an unsigned {@code short}
   * @throws IllegalArgumentException if {@code i} cannot be represented as an unsigned {@code
   *     short}
   */
  public static int requireUnsignedShort(int i) {
    if (i > UNSIGNED_SHORT_MAX_VALUE) {
      throw new IllegalArgumentException(
          String.format("%d is larger than %d bits", i, UNSIGNED_SHORT_SIZE));
    }

    return i;
  }

  /**
   * Encode an unsigned medium integer on 3 bytes / 24 bits. This can be decoded directly by the
   * {@link ByteBuf#readUnsignedMedium()} method.
   *
   * @param byteBuf the {@link ByteBuf} into which to write the bits
   * @param i the medium integer to encode
   * @see #requireUnsignedMedium(int)
   */
  public static void encodeUnsignedMedium(ByteBuf byteBuf, int i) {
    requireUnsignedMedium(i);
    // Write each byte separately in reverse order, this mean we can write 1 << 23 without
    // overflowing.
    byteBuf.writeByte(i >> 16);
    byteBuf.writeByte(i >> 8);
    byteBuf.writeByte(i);
  }
}
