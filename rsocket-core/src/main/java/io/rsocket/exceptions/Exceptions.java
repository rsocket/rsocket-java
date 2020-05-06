/*
 * Copyright 2015-2020 the original author or authors.
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
import static io.rsocket.frame.ErrorFrameCodec.MAX_USER_ALLOWED_ERROR_CODE;
import static io.rsocket.frame.ErrorFrameCodec.MIN_USER_ALLOWED_ERROR_CODE;
import static io.rsocket.frame.ErrorFrameCodec.REJECTED;
import static io.rsocket.frame.ErrorFrameCodec.REJECTED_RESUME;
import static io.rsocket.frame.ErrorFrameCodec.REJECTED_SETUP;
import static io.rsocket.frame.ErrorFrameCodec.UNSUPPORTED_SETUP;

import io.netty.buffer.ByteBuf;
import io.rsocket.RSocketErrorException;
import io.rsocket.frame.ErrorFrameCodec;
import java.util.Objects;

/** Utility class that generates an exception from a frame. */
public final class Exceptions {

  private Exceptions() {}

  /**
   * Create a {@link RSocketErrorException} from a Frame that matches the error code it contains.
   *
   * @param frame the frame to retrieve the error code and message from
   * @return a {@link RSocketErrorException} that matches the error code in the Frame
   * @throws NullPointerException if {@code frame} is {@code null}
   */
  public static RuntimeException from(int streamId, ByteBuf frame) {
    Objects.requireNonNull(frame, "frame must not be null");

    int errorCode = ErrorFrameCodec.errorCode(frame);
    String message = ErrorFrameCodec.dataUtf8(frame);

    if (streamId == 0) {
      switch (errorCode) {
        case INVALID_SETUP:
          return new InvalidSetupException(message);
        case UNSUPPORTED_SETUP:
          return new UnsupportedSetupException(message);
        case REJECTED_SETUP:
          return new RejectedSetupException(message);
        case REJECTED_RESUME:
          return new RejectedResumeException(message);
        case CONNECTION_ERROR:
          return new ConnectionErrorException(message);
        case CONNECTION_CLOSE:
          return new ConnectionCloseException(message);
        default:
          return new IllegalArgumentException(
              String.format("Invalid Error frame in Stream ID 0: 0x%08X '%s'", errorCode, message));
      }
    } else {
      switch (errorCode) {
        case APPLICATION_ERROR:
          return new ApplicationErrorException(message);
        case REJECTED:
          return new RejectedException(message);
        case CANCELED:
          return new CanceledException(message);
        case INVALID:
          return new InvalidException(message);
        default:
          if (errorCode >= MIN_USER_ALLOWED_ERROR_CODE
              || errorCode <= MAX_USER_ALLOWED_ERROR_CODE) {
            return new CustomRSocketException(errorCode, message);
          }
          return new IllegalArgumentException(
              String.format(
                  "Invalid Error frame in Stream ID %d: 0x%08X '%s'",
                  streamId, errorCode, message));
      }
    }
  }
}
