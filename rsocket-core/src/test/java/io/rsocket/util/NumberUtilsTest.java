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

import static org.assertj.core.api.Assertions.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

final class NumberUtilsTest {

  @DisplayName("returns int value with postitive int")
  @Test
  void requireNonNegativeInt() {
    assertThat(NumberUtils.requireNonNegative(Integer.MAX_VALUE, "test-message"))
        .isEqualTo(Integer.MAX_VALUE);
  }

  @DisplayName(
      "requireNonNegative with int argument throws IllegalArgumentException with negative value")
  @Test
  void requireNonNegativeIntNegative() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> NumberUtils.requireNonNegative(Integer.MIN_VALUE, "test-message"))
        .withMessage("test-message");
  }

  @DisplayName("requireNonNegative with int argument throws NullPointerException with null message")
  @Test
  void requireNonNegativeIntNullMessage() {
    assertThatNullPointerException()
        .isThrownBy(() -> NumberUtils.requireNonNegative(Integer.MIN_VALUE, null))
        .withMessage("message must not be null");
  }

  @DisplayName("requireNonNegative returns int value with zero")
  @Test
  void requireNonNegativeIntZero() {
    assertThat(NumberUtils.requireNonNegative(0, "test-message")).isEqualTo(0);
  }

  @DisplayName("requirePositive returns int value with positive int")
  @Test
  void requirePositiveInt() {
    assertThat(NumberUtils.requirePositive(Integer.MAX_VALUE, "test-message"))
        .isEqualTo(Integer.MAX_VALUE);
  }

  @DisplayName(
      "requirePositive with int argument throws IllegalArgumentException with negative value")
  @Test
  void requirePositiveIntNegative() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> NumberUtils.requirePositive(Integer.MIN_VALUE, "test-message"))
        .withMessage("test-message");
  }

  @DisplayName("requirePositive with int argument throws NullPointerException with null message")
  @Test
  void requirePositiveIntNullMessage() {
    assertThatNullPointerException()
        .isThrownBy(() -> NumberUtils.requirePositive(Integer.MIN_VALUE, null))
        .withMessage("message must not be null");
  }

  @DisplayName("requirePositive with int argument throws IllegalArgumentException with zero value")
  @Test
  void requirePositiveIntZero() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> NumberUtils.requirePositive(0, "test-message"))
        .withMessage("test-message");
  }

  @DisplayName("requirePositive returns long value with positive long")
  @Test
  void requirePositiveLong() {
    assertThat(NumberUtils.requirePositive(Long.MAX_VALUE, "test-message"))
        .isEqualTo(Long.MAX_VALUE);
  }

  @DisplayName(
      "requirePositive with long argument throws IllegalArgumentException with negative value")
  @Test
  void requirePositiveLongNegative() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> NumberUtils.requirePositive(Long.MIN_VALUE, "test-message"))
        .withMessage("test-message");
  }

  @DisplayName("requirePositive with long argument throws NullPointerException with null message")
  @Test
  void requirePositiveLongNullMessage() {
    assertThatNullPointerException()
        .isThrownBy(() -> NumberUtils.requirePositive(Long.MIN_VALUE, null))
        .withMessage("message must not be null");
  }

  @DisplayName("requirePositive with long argument throws IllegalArgumentException with zero value")
  @Test
  void requirePositiveLongZero() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> NumberUtils.requirePositive(0L, "test-message"))
        .withMessage("test-message");
  }

  @DisplayName("requireUnsignedByte returns length if 255")
  @Test
  void requireUnsignedByte() {
    assertThat(NumberUtils.requireUnsignedByte((1 << 8) - 1)).isEqualTo(255);
  }

  @DisplayName("requireUnsignedByte throws IllegalArgumentException if larger than 255")
  @Test
  void requireUnsignedByteOverFlow() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> NumberUtils.requireUnsignedByte(1 << 8))
        .withMessage("%d is larger than 8 bits", 1 << 8);
  }

  @DisplayName("requireUnsignedMedium returns length if 16_777_215")
  @Test
  void requireUnsignedMedium() {
    assertThat(NumberUtils.requireUnsignedMedium((1 << 24) - 1)).isEqualTo(16_777_215);
  }

  @DisplayName("requireUnsignedMedium throws IllegalArgumentException if larger than 16_777_215")
  @Test
  void requireUnsignedMediumOverFlow() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> NumberUtils.requireUnsignedMedium(1 << 24))
        .withMessage("%d is larger than 24 bits", 1 << 24);
  }

  @DisplayName("requireUnsignedShort returns length if 65_535")
  @Test
  void requireUnsignedShort() {
    assertThat(NumberUtils.requireUnsignedShort((1 << 16) - 1)).isEqualTo(65_535);
  }

  @DisplayName("requireUnsignedShort throws IllegalArgumentException if larger than 65_535")
  @Test
  void requireUnsignedShortOverFlow() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> NumberUtils.requireUnsignedShort(1 << 16))
        .withMessage("%d is larger than 16 bits", 1 << 16);
  }

  @Test
  void encodeUnsignedMedium() {
    ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
    NumberUtils.encodeUnsignedMedium(buffer, 129);
    buffer.markReaderIndex();

    assertThat(buffer.readUnsignedMedium()).as("reading as unsigned medium").isEqualTo(129);

    buffer.resetReaderIndex();
    assertThat(buffer.readMedium()).as("reading as signed medium").isEqualTo(129);
  }

  @Test
  void encodeUnsignedMediumLarge() {
    ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
    NumberUtils.encodeUnsignedMedium(buffer, 0xFFFFFC);
    buffer.markReaderIndex();

    assertThat(buffer.readUnsignedMedium()).as("reading as unsigned medium").isEqualTo(16777212);

    buffer.resetReaderIndex();
    assertThat(buffer.readMedium()).as("reading as signed medium").isEqualTo(-4);
  }
}
