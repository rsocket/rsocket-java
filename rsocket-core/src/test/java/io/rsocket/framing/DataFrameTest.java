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

interface DataFrameTest<T extends DataFrame> extends FrameTest<T> {

  @DisplayName("return data as UTF-8")
  @Test
  default void getDataAsUtf8() {
    Tuple2<T, ByteBuf> tuple = getFrameWithData();
    T frame = tuple.getT1();
    ByteBuf data = tuple.getT2();

    assertThat(frame.getDataAsUtf8()).isEqualTo(data.toString(UTF_8));
  }

  @DisplayName("returns empty data as UTF-8")
  @Test
  default void getDataAsUtf8Empty() {
    T frame = getFrameWithEmptyData();

    assertThat(frame.getDataAsUtf8()).isEqualTo("");
  }

  @DisplayName("returns data length")
  @Test
  default void getDataLength() {
    Tuple2<T, ByteBuf> tuple = getFrameWithData();
    T frame = tuple.getT1();
    ByteBuf data = tuple.getT2();

    assertThat(frame.getDataLength()).isEqualTo(data.readableBytes());
  }

  @DisplayName("returns empty data length")
  @Test
  default void getDataLengthEmpty() {
    T frame = getFrameWithEmptyData();

    assertThat(frame.getDataLength()).isEqualTo(0);
  }

  Tuple2<T, ByteBuf> getFrameWithData();

  T getFrameWithEmptyData();

  @DisplayName("returns unsafe data")
  @Test
  default void getUnsafeData() {
    Tuple2<T, ByteBuf> tuple = getFrameWithData();
    T frame = tuple.getT1();
    ByteBuf data = tuple.getT2();

    assertThat(frame.getUnsafeData()).isEqualTo(data);
  }

  @DisplayName("returns unsafe empty data")
  @Test
  default void getUnsafeDataEmpty() {
    T frame = getFrameWithEmptyData();

    assertThat(frame.getUnsafeData()).isEqualTo(EMPTY_BUFFER);
  }

  @DisplayName("maps data")
  @Test
  default void mapData() {
    Tuple2<T, ByteBuf> tuple = getFrameWithData();
    T frame = tuple.getT1();
    ByteBuf data = tuple.getT2();

    assertThat(frame.mapData(Function.identity())).isEqualTo(data);
  }

  @DisplayName("maps empty data")
  @Test
  default void mapDataEmpty() {
    T frame = getFrameWithEmptyData();

    assertThat(frame.mapData(Function.identity())).isEqualTo(EMPTY_BUFFER);
  }

  @DisplayName("mapData throws NullPointerException with null function")
  @Test
  default void mapDataNullFunction() {
    T frame = getFrameWithEmptyData();

    assertThatNullPointerException()
        .isThrownBy(() -> frame.mapData(null))
        .withMessage("function must not be null");
  }
}
