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

import static io.rsocket.util.NumberUtils.requireUnsignedByte;
import static io.rsocket.util.NumberUtils.requireUnsignedMedium;
import static io.rsocket.util.NumberUtils.requireUnsignedShort;

import io.netty.buffer.ByteBuf;
import java.util.Objects;

/** Utilities for working with {@code ByteBuf} lengths */
final class LengthUtils {

  private LengthUtils() {}

  /**
   * Returns the length of a {@link ByteBuf} as an unsigned {@code byte}.
   *
   * @param byteBuf the {@link ByteBuf} to get the length of
   * @return the length of a {@link ByteBuf} as an unsigned {@code byte}
   */
  static int getLengthAsUnsignedByte(ByteBuf byteBuf) {
    Objects.requireNonNull(byteBuf, "byteBuf must not be null");

    return requireUnsignedByte(byteBuf.readableBytes());
  }

  /**
   * Returns the length of a {@link ByteBuf} as an unsigned {@code medium}
   *
   * @param byteBuf the {@link ByteBuf} to get the length of
   * @return the length of a {@link ByteBuf} as an unsigned {@code medium}
   */
  static int getLengthAsUnsignedMedium(ByteBuf byteBuf) {
    Objects.requireNonNull(byteBuf, "byteBuf must not be null");

    return requireUnsignedMedium(byteBuf.readableBytes());
  }

  /**
   * Returns the length of a {@link ByteBuf} as an unsigned {@code short}
   *
   * @param byteBuf the {@link ByteBuf} to get the length of
   * @return the length of a {@link ByteBuf} as an unsigned {@code short}
   */
  static int getLengthAsUnsignedShort(ByteBuf byteBuf) {
    Objects.requireNonNull(byteBuf, "byteBuf must not be null");

    return requireUnsignedShort(byteBuf.readableBytes());
  }
}
