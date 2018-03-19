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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import io.netty.buffer.ByteBuf;
import java.util.function.Function;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.util.function.Tuple2;

interface FrameTest<T extends Frame> {

  @DisplayName("consumes frame")
  @Test
  default void consumeFrame() {
    Tuple2<T, ByteBuf> tuple = getFrame();
    T frame = tuple.getT1();
    ByteBuf byteBuf = tuple.getT2();

    frame.consumeFrame(frameByteBuf -> assertThat(frameByteBuf).isEqualTo(byteBuf));
  }

  @DisplayName("consumeFrame throws NullPointerException with null function")
  @Test
  default void consumeFrameNullFunction() {
    Tuple2<T, ByteBuf> tuple = getFrame();
    T frame = tuple.getT1();

    assertThatNullPointerException()
        .isThrownBy(() -> frame.consumeFrame(null))
        .withMessage("consumer must not be null");
  }

  @DisplayName("creates frame from ByteBuf")
  @Test
  default void createFrameFromByteBuf() {
    Tuple2<T, ByteBuf> tuple = getFrame();
    T frame = tuple.getT1();
    ByteBuf byteBuf = tuple.getT2();

    assertThat(getCreateFrameFromByteBuf().apply(byteBuf)).isEqualTo(frame);
  }

  @DisplayName("create frame from ByteBuf throws NullPointerException with null ByteBuf")
  @Test
  default void createFrameFromByteBufNullByteBuf() {
    assertThatNullPointerException()
        .isThrownBy(() -> getCreateFrameFromByteBuf().apply(null))
        .withMessage("byteBuf must not be null");
  }

  Function<ByteBuf, T> getCreateFrameFromByteBuf();

  Tuple2<T, ByteBuf> getFrame();

  @DisplayName("maps frame")
  @Test
  default void mapFrame() {
    Tuple2<T, ByteBuf> tuple = getFrame();
    T frame = tuple.getT1();
    ByteBuf byteBuf = tuple.getT2();

    assertThat(frame.mapFrame(Function.identity())).isEqualTo(byteBuf);
  }

  @DisplayName("mapFrame throws NullPointerException with null function")
  @Test
  default void mapFrameNullFunction() {
    Tuple2<T, ByteBuf> tuple = getFrame();
    T frame = tuple.getT1();

    assertThatNullPointerException()
        .isThrownBy(() -> frame.mapFrame(null))
        .withMessage("function must not be null");
  }
}
