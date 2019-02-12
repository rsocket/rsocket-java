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

final class ExceptionsTest {
  /*
  @DisplayName("from returns ApplicationErrorException")
  @Test
  void fromApplicationException() {
    ByteBuf byteBuf = createErrorFrame(APPLICATION_ERROR, "test-message");

    assertThat(Exceptions.from(Frame.from(byteBuf)))
        .isInstanceOf(ApplicationErrorException.class)
        .withFailMessage("test-message");
  }

  @DisplayName("from returns CanceledException")
  @Test
  void fromCanceledException() {
    ByteBuf byteBuf = createErrorFrame(CANCELED, "test-message");

    assertThat(Exceptions.from(Frame.from(byteBuf)))
        .isInstanceOf(CanceledException.class)
        .withFailMessage("test-message");
  }

  @DisplayName("from returns ConnectionCloseException")
  @Test
  void fromConnectionCloseException() {
    ByteBuf byteBuf = createErrorFrame(CONNECTION_CLOSE, "test-message");

    assertThat(Exceptions.from(Frame.from(byteBuf)))
        .isInstanceOf(ConnectionCloseException.class)
        .withFailMessage("test-message");
  }

  @DisplayName("from returns ConnectionErrorException")
  @Test
  void fromConnectionErrorException() {
    ByteBuf byteBuf = createErrorFrame(CONNECTION_ERROR, "test-message");

    assertThat(Exceptions.from(Frame.from(byteBuf)))
        .isInstanceOf(ConnectionErrorException.class)
        .withFailMessage("test-message");
  }

  @DisplayName("from returns IllegalArgumentException if error frame has illegal error code")
  @Test
  void fromIllegalErrorFrame() {
    ByteBuf byteBuf = createErrorFrame(0x00000000, "test-message");

    assertThat(Exceptions.from(Frame.from(byteBuf)))
        .isInstanceOf(IllegalArgumentException.class)
        .withFailMessage("Invalid Error frame: %d, '%s'", 0, "test-message");
  }

  @DisplayName("from returns InvalidException")
  @Test
  void fromInvalidException() {
    ByteBuf byteBuf = createErrorFrame(INVALID, "test-message");

    assertThat(Exceptions.from(Frame.from(byteBuf)))
        .isInstanceOf(InvalidException.class)
        .withFailMessage("test-message");
  }

  @DisplayName("from returns InvalidSetupException")
  @Test
  void fromInvalidSetupException() {
    ByteBuf byteBuf = createErrorFrame(INVALID_SETUP, "test-message");

    assertThat(Exceptions.from(Frame.from(byteBuf)))
        .isInstanceOf(InvalidSetupException.class)
        .withFailMessage("test-message");
  }

  @DisplayName("from returns RejectedException")
  @Test
  void fromRejectedException() {
    ByteBuf byteBuf = createErrorFrame(REJECTED, "test-message");

    assertThat(Exceptions.from(Frame.from(byteBuf)))
        .isInstanceOf(RejectedException.class)
        .withFailMessage("test-message");
  }

  @DisplayName("from returns RejectedResumeException")
  @Test
  void fromRejectedResumeException() {
    ByteBuf byteBuf = createErrorFrame(REJECTED_RESUME, "test-message");

    assertThat(Exceptions.from(Frame.from(byteBuf)))
        .isInstanceOf(RejectedResumeException.class)
        .withFailMessage("test-message");
  }

  @DisplayName("from returns RejectedSetupException")
  @Test
  void fromRejectedSetupException() {
    ByteBuf byteBuf = createErrorFrame(REJECTED_SETUP, "test-message");

    assertThat(Exceptions.from(Frame.from(byteBuf)))
        .isInstanceOf(RejectedSetupException.class)
        .withFailMessage("test-message");
  }

  @DisplayName("from returns UnsupportedSetupException")
  @Test
  void fromUnsupportedSetupException() {
    ByteBuf byteBuf = createErrorFrame(UNSUPPORTED_SETUP, "test-message");

    assertThat(Exceptions.from(Frame.from(byteBuf)))
        .isInstanceOf(UnsupportedSetupException.class)
        .withFailMessage("test-message");
  }

  @DisplayName("from throws NullPointerException with null frame")
  @Test
  void fromWithNullFrame() {
    assertThatNullPointerException()
        .isThrownBy(() -> Exceptions.from(null))
        .withMessage("frame must not be null");
  }

  private ByteBuf createErrorFrame(int errorCode, String message) {
    ByteBuf byteBuf = Unpooled.buffer();

    ErrorFrameFlyweight.encode(byteBuf, 0, errorCode, Unpooled.copiedBuffer(message, UTF_8));

    return byteBuf;
  }*/
}
