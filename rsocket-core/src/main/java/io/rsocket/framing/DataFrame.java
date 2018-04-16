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

import static java.nio.charset.StandardCharsets.UTF_8;

import io.netty.buffer.ByteBuf;
import java.util.Objects;
import java.util.function.Function;

/** An RSocket frame that only contains data. */
public interface DataFrame extends Frame {

  /**
   * Returns the data as a UTF-8 {@link String}.
   *
   * @return the data as a UTF-8 {@link String}
   */
  default String getDataAsUtf8() {
    return getUnsafeData().toString(UTF_8);
  }

  /**
   * Returns the length of the data in the frame.
   *
   * @return the length of the data in the frame
   */
  default int getDataLength() {
    return getUnsafeData().readableBytes();
  }

  /**
   * Returns the data directly.
   *
   * <p><b>Note:</b> this data will be outside of the {@link Frame}'s lifecycle and may be released
   * at any time. It is highly recommended that you {@link ByteBuf#retain()} the data if you store
   * it.
   *
   * @return the data directly
   * @see #getDataAsUtf8()
   * @see #mapData(Function)
   */
  ByteBuf getUnsafeData();

  /**
   * Exposes the data for mapping to a different type.
   *
   * @param function the function to transform the data to a different type
   * @param <T> the different type
   * @return the data mapped to a different type
   * @throws NullPointerException if {@code function} is {@code null}
   */
  default <T> T mapData(Function<ByteBuf, T> function) {
    Objects.requireNonNull(function, "function must not be null");

    return function.apply(getUnsafeData());
  }
}
