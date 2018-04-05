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

import static io.netty.buffer.Unpooled.EMPTY_BUFFER;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import io.netty.buffer.ByteBuf;
import java.util.function.Function;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.util.function.Tuple2;

interface MetadataFrameTest<T extends MetadataFrame> extends FrameTest<T> {

  T getFrameWithEmptyMetadata();

  Tuple2<T, ByteBuf> getFrameWithMetadata();

  T getFrameWithoutMetadata();

  @DisplayName("returns metadata as UTF-8")
  @Test
  default void getMetadataAsUtf8() {
    Tuple2<T, ByteBuf> tuple = getFrameWithMetadata();
    T frame = tuple.getT1();
    ByteBuf metadata = tuple.getT2();

    assertThat(frame.getMetadataAsUtf8()).hasValue(metadata.toString(UTF_8));
  }

  @DisplayName("returns empty optional metadata as UTF-8")
  @Test
  default void getMetadataAsUtf8Empty() {
    T frame = getFrameWithEmptyMetadata();

    assertThat(frame.getMetadataAsUtf8()).hasValue("");
  }

  @DisplayName("returns empty optional for metadata as UTF-8")
  @Test
  default void getMetadataAsUtf8NoFlag() {
    T frame = getFrameWithoutMetadata();

    assertThat(frame.getMetadataAsUtf8()).isEmpty();
  }

  @DisplayName("returns metadata length")
  @Test
  default void getMetadataLength() {
    Tuple2<T, ByteBuf> tuple = getFrameWithMetadata();
    T frame = tuple.getT1();
    ByteBuf metadata = tuple.getT2();

    assertThat(frame.getMetadataLength()).hasValue(metadata.readableBytes());
  }

  @DisplayName("returns empty optional metadata length")
  @Test
  default void getMetadataLengthEmpty() {
    T frame = getFrameWithEmptyMetadata();

    assertThat(frame.getMetadataLength()).hasValue(0);
  }

  @DisplayName("returns empty optional for metadata length")
  @Test
  default void getMetadataLengthNoFlag() {
    T frame = getFrameWithoutMetadata();

    assertThat(frame.getMetadataLength()).isEmpty();
  }

  @DisplayName("returns unsafe metadata")
  @Test
  default void getUnsafeMetadata() {
    Tuple2<T, ByteBuf> tuple = getFrameWithMetadata();
    T frame = tuple.getT1();
    ByteBuf metadata = tuple.getT2();

    assertThat(frame.getUnsafeMetadata()).isEqualTo(metadata);
  }

  @DisplayName("returns unsafe metadata as UTF-8")
  @Test
  default void getUnsafeMetadataAsUtf8() {
    Tuple2<T, ByteBuf> tuple = getFrameWithMetadata();
    T frame = tuple.getT1();
    ByteBuf metadata = tuple.getT2();

    assertThat(frame.getUnsafeMetadataAsUtf8()).isEqualTo(metadata.toString(UTF_8));
  }

  @DisplayName("returns empty unsafe metadata as UTF-8")
  @Test
  default void getUnsafeMetadataAsUtf8Empty() {
    T frame = getFrameWithEmptyMetadata();

    assertThat(frame.getUnsafeMetadataAsUtf8()).isEqualTo("");
  }

  @DisplayName("returns null for unsafe metadata as UTF-8")
  @Test
  default void getUnsafeMetadataAsUtf8NoFlag() {
    T frame = getFrameWithoutMetadata();

    assertThat(frame.getUnsafeMetadataAsUtf8()).isNull();
  }

  @DisplayName("returns unsafe empty metadata")
  @Test
  default void getUnsafeMetadataEmpty() {
    T frame = getFrameWithEmptyMetadata();

    assertThat(frame.getUnsafeMetadata()).isEqualTo(EMPTY_BUFFER);
  }

  @DisplayName("returns unsafe metadata length")
  @Test
  default void getUnsafeMetadataLength() {
    Tuple2<T, ByteBuf> tuple = getFrameWithMetadata();
    T frame = tuple.getT1();
    ByteBuf metadata = tuple.getT2();

    assertThat(frame.getUnsafeMetadataLength()).isEqualTo(metadata.readableBytes());
  }

  @DisplayName("returns unsafe empty metadata length")
  @Test
  default void getUnsafeMetadataLengthEmpty() {
    T frame = getFrameWithEmptyMetadata();

    assertThat(frame.getUnsafeMetadataLength()).isEqualTo(0);
  }

  @DisplayName("returns null for unsafe metadata length")
  @Test
  default void getUnsafeMetadataLengthNoFlag() {
    T frame = getFrameWithoutMetadata();

    assertThat(frame.getUnsafeMetadataLength()).isNull();
  }

  @DisplayName("returns null for unsafe metadata")
  @Test
  default void getUnsafeMetadataNoFlag() {
    T frame = getFrameWithoutMetadata();

    assertThat(frame.getUnsafeMetadata()).isNull();
  }

  @DisplayName("maps metadata")
  @Test
  default void mapMetadata() {
    Tuple2<T, ByteBuf> tuple = getFrameWithMetadata();
    T frame = tuple.getT1();
    ByteBuf metadata = tuple.getT2();

    assertThat(frame.mapMetadata(Function.identity())).hasValue(metadata);
  }

  @DisplayName("maps empty metadata")
  @Test
  default void mapMetadataEmpty() {
    T frame = getFrameWithEmptyMetadata();

    assertThat(frame.mapMetadata(Function.identity())).hasValue(EMPTY_BUFFER);
  }

  @DisplayName("maps empty optional for metadata")
  @Test
  default void mapMetadataNoFlag() {
    T frame = getFrameWithoutMetadata();

    assertThat(frame.mapMetadata(Function.identity())).isEmpty();
  }

  @DisplayName("mapMetadata throws NullPointerException with null function")
  @Test
  default void mapMetadataNullFunction() {
    T frame = getFrameWithEmptyMetadata();

    assertThatNullPointerException()
        .isThrownBy(() -> frame.mapMetadata(null))
        .withMessage("function must not be null");
  }
}
