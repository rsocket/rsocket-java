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

package io.rsocket.exceptions;

import static io.rsocket.frame.ErrorFrameCodec.APPLICATION_ERROR;
import static io.rsocket.frame.ErrorFrameCodec.CANCELED;
import static io.rsocket.frame.ErrorFrameCodec.CONNECTION_CLOSE;
import static io.rsocket.frame.ErrorFrameCodec.CONNECTION_ERROR;
import static io.rsocket.frame.ErrorFrameCodec.INVALID;
import static io.rsocket.frame.ErrorFrameCodec.INVALID_SETUP;
import static io.rsocket.frame.ErrorFrameCodec.REJECTED;
import static io.rsocket.frame.ErrorFrameCodec.REJECTED_RESUME;
import static io.rsocket.frame.ErrorFrameCodec.REJECTED_SETUP;
import static io.rsocket.frame.ErrorFrameCodec.UNSUPPORTED_SETUP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.rsocket.frame.ErrorFrameCodec;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

final class ExceptionsTest {
  @DisplayName("from returns ApplicationErrorException")
  @Test
  void fromApplicationException() {
    ByteBuf byteBuf = createErrorFrame(1, APPLICATION_ERROR, "test-message");

    assertThat(Exceptions.from(1, byteBuf))
        .isInstanceOf(ApplicationErrorException.class)
        .hasMessage("test-message");

    assertThat(Exceptions.from(0, byteBuf))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid Error frame in Stream ID 0: 0x%08X '%s'", APPLICATION_ERROR, "test-message");
  }

  @DisplayName("from returns CanceledException")
  @Test
  void fromCanceledException() {
    ByteBuf byteBuf = createErrorFrame(1, CANCELED, "test-message");

    assertThat(Exceptions.from(1, byteBuf))
        .isInstanceOf(CanceledException.class)
        .hasMessage("test-message");

    assertThat(Exceptions.from(0, byteBuf))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid Error frame in Stream ID 0: 0x%08X '%s'", CANCELED, "test-message");
  }

  @DisplayName("from returns ConnectionCloseException")
  @Test
  void fromConnectionCloseException() {
    ByteBuf byteBuf = createErrorFrame(0, CONNECTION_CLOSE, "test-message");

    assertThat(Exceptions.from(0, byteBuf))
        .isInstanceOf(ConnectionCloseException.class)
        .hasMessage("test-message");

    assertThat(Exceptions.from(1, byteBuf))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid Error frame in Stream ID 1: 0x%08X '%s'", CONNECTION_CLOSE, "test-message");
  }

  @DisplayName("from returns ConnectionErrorException")
  @Test
  void fromConnectionErrorException() {
    ByteBuf byteBuf = createErrorFrame(0, CONNECTION_ERROR, "test-message");

    assertThat(Exceptions.from(0, byteBuf))
        .isInstanceOf(ConnectionErrorException.class)
        .hasMessage("test-message");

    assertThat(Exceptions.from(1, byteBuf))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid Error frame in Stream ID 1: 0x%08X '%s'", CONNECTION_ERROR, "test-message");
  }

  @DisplayName("from returns IllegalArgumentException if error frame has illegal error code")
  @Test
  void fromIllegalErrorFrame() {
    ByteBuf byteBuf = createErrorFrame(0, 0x00000000, "test-message");

    assertThat(Exceptions.from(0, byteBuf))
        .hasMessage("Invalid Error frame in Stream ID 0: 0x%08X '%s'", 0, "test-message")
        .isInstanceOf(IllegalArgumentException.class);

    assertThat(Exceptions.from(1, byteBuf))
        .hasMessage("Invalid Error frame in Stream ID 1: 0x%08X '%s'", 0x00000000, "test-message")
        .isInstanceOf(IllegalArgumentException.class);
  }

  @DisplayName("from returns InvalidException")
  @Test
  void fromInvalidException() {
    ByteBuf byteBuf = createErrorFrame(1, INVALID, "test-message");

    assertThat(Exceptions.from(1, byteBuf))
        .isInstanceOf(InvalidException.class)
        .hasMessage("test-message");

    assertThat(Exceptions.from(0, byteBuf))
        .hasMessage("Invalid Error frame in Stream ID 0: 0x%08X '%s'", INVALID, "test-message")
        .isInstanceOf(IllegalArgumentException.class);
  }

  @DisplayName("from returns InvalidSetupException")
  @Test
  void fromInvalidSetupException() {
    ByteBuf byteBuf = createErrorFrame(0, INVALID_SETUP, "test-message");

    assertThat(Exceptions.from(0, byteBuf))
        .isInstanceOf(InvalidSetupException.class)
        .hasMessage("test-message");

    assertThat(Exceptions.from(1, byteBuf))
        .hasMessage(
            "Invalid Error frame in Stream ID 1: 0x%08X '%s'", INVALID_SETUP, "test-message")
        .isInstanceOf(IllegalArgumentException.class);
  }

  @DisplayName("from returns RejectedException")
  @Test
  void fromRejectedException() {
    ByteBuf byteBuf = createErrorFrame(1, REJECTED, "test-message");

    assertThat(Exceptions.from(1, byteBuf))
        .isInstanceOf(RejectedException.class)
        .withFailMessage("test-message");

    assertThat(Exceptions.from(0, byteBuf))
        .hasMessage("Invalid Error frame in Stream ID 0: 0x%08X '%s'", REJECTED, "test-message")
        .isInstanceOf(IllegalArgumentException.class);
  }

  @DisplayName("from returns RejectedResumeException")
  @Test
  void fromRejectedResumeException() {
    ByteBuf byteBuf = createErrorFrame(0, REJECTED_RESUME, "test-message");

    assertThat(Exceptions.from(0, byteBuf))
        .isInstanceOf(RejectedResumeException.class)
        .hasMessage("test-message");

    assertThat(Exceptions.from(1, byteBuf))
        .hasMessage(
            "Invalid Error frame in Stream ID 1: 0x%08X '%s'", REJECTED_RESUME, "test-message")
        .isInstanceOf(IllegalArgumentException.class);
  }

  @DisplayName("from returns RejectedSetupException")
  @Test
  void fromRejectedSetupException() {
    ByteBuf byteBuf = createErrorFrame(0, REJECTED_SETUP, "test-message");

    assertThat(Exceptions.from(0, byteBuf))
        .isInstanceOf(RejectedSetupException.class)
        .withFailMessage("test-message");

    assertThat(Exceptions.from(1, byteBuf))
        .hasMessage(
            "Invalid Error frame in Stream ID 1: 0x%08X '%s'", REJECTED_SETUP, "test-message")
        .isInstanceOf(IllegalArgumentException.class);
  }

  @DisplayName("from returns UnsupportedSetupException")
  @Test
  void fromUnsupportedSetupException() {
    ByteBuf byteBuf = createErrorFrame(0, UNSUPPORTED_SETUP, "test-message");

    assertThat(Exceptions.from(0, byteBuf))
        .isInstanceOf(UnsupportedSetupException.class)
        .hasMessage("test-message");

    assertThat(Exceptions.from(1, byteBuf))
        .hasMessage(
            "Invalid Error frame in Stream ID 1: 0x%08X '%s'", UNSUPPORTED_SETUP, "test-message")
        .isInstanceOf(IllegalArgumentException.class);
  }

  @DisplayName("from returns CustomRSocketException")
  @Test
  void fromCustomRSocketException() {
    for (int i = 0; i < 1000; i++) {
      int randomCode =
          ThreadLocalRandom.current().nextBoolean()
              ? ThreadLocalRandom.current()
                  .nextInt(Integer.MIN_VALUE, ErrorFrameCodec.MAX_USER_ALLOWED_ERROR_CODE)
              : ThreadLocalRandom.current()
                  .nextInt(ErrorFrameCodec.MIN_USER_ALLOWED_ERROR_CODE, Integer.MAX_VALUE);
      ByteBuf byteBuf = createErrorFrame(0, randomCode, "test-message");

      assertThat(Exceptions.from(1, byteBuf))
          .isInstanceOf(CustomRSocketException.class)
          .hasMessage("test-message");

      assertThat(Exceptions.from(0, byteBuf))
          .hasMessage("Invalid Error frame in Stream ID 0: 0x%08X '%s'", randomCode, "test-message")
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  @DisplayName("from throws NullPointerException with null frame")
  @Test
  void fromWithNullFrame() {
    assertThatNullPointerException()
        .isThrownBy(() -> Exceptions.from(0, null))
        .withMessage("frame must not be null");
  }

  private ByteBuf createErrorFrame(int streamId, int errorCode, String message) {
    return ErrorFrameCodec.encode(
        UnpooledByteBufAllocator.DEFAULT, streamId, new TestRSocketException(errorCode, message));
  }
}
