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
import java.util.Optional;
import java.util.function.Function;
import reactor.util.annotation.Nullable;

/** An RSocket frame that only contains metadata. */
public interface MetadataFrame extends Frame {

  /**
   * Returns the metadata as a UTF-8 {@link String}. If the Metadata flag is not set, returns {@link
   * Optional#empty()}.
   *
   * @return optionally, the metadata as a UTF-8 {@link String}
   */
  default Optional<String> getMetadataAsUtf8() {
    return Optional.ofNullable(getUnsafeMetadataAsUtf8());
  }

  /**
   * Returns the length of the metadata in the frame. If the Metadata flag is not set, returns
   * {@link Optional#empty()}.
   *
   * @return optionally, the length of the metadata in the frame
   */
  default Optional<Integer> getMetadataLength() {
    return Optional.ofNullable(getUnsafeMetadataLength());
  }

  /**
   * Returns the metadata directly. If the Metadata flag is not set, returns {@code null}.
   *
   * <p><b>Note:</b> this metadata will be outside of the {@link Frame}'s lifecycle and may be
   * released at any time. It is highly recommended that you {@link ByteBuf#retain()} the metadata
   * if you store it.
   *
   * @return the metadata directly, or {@code null} if the Metadata flag is not set
   * @see #mapMetadata(Function)
   */
  @Nullable
  ByteBuf getUnsafeMetadata();

  /**
   * Returns the metadata as a UTF-8 {@link String}. If the Metadata flag is not set, returns {@code
   * null}.
   *
   * @return the metadata as a UTF-8 {@link String} or {@code null} if the Metadata flag is not set.
   * @see #getMetadataAsUtf8()
   */
  default @Nullable String getUnsafeMetadataAsUtf8() {
    ByteBuf byteBuf = getUnsafeMetadata();
    return byteBuf == null ? null : byteBuf.toString(UTF_8);
  }

  /**
   * Returns the length of the metadata in the frame directly. If the Metadata flag is not set,
   * returns {@code null}.
   *
   * @return the length of the metadata in frame directly, or {@code null} if the Metadata flag is
   *     not set
   * @see #getMetadataLength()
   */
  default @Nullable Integer getUnsafeMetadataLength() {
    ByteBuf byteBuf = getUnsafeMetadata();
    return byteBuf == null ? null : byteBuf.readableBytes();
  }

  /**
   * Exposes the metadata for mapping to a different type. If the Metadata flag is not set, returns
   * {@link Optional#empty()}.
   *
   * @param function the function to transform the metadata to a different type
   * @param <T> the different type
   * @return optionally, the metadata mapped to a different type
   * @throws NullPointerException if {@code function} is {@code null}
   */
  default <T> Optional<T> mapMetadata(Function<ByteBuf, T> function) {
    Objects.requireNonNull(function, "function must not be null");

    return Optional.ofNullable(getUnsafeMetadata()).map(function);
  }
}
