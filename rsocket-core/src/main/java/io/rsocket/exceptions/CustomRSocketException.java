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

import io.rsocket.frame.ErrorFrameCodec;
import reactor.util.annotation.Nullable;

public class CustomRSocketException extends RSocketException {
  private static final long serialVersionUID = 7873267740343446585L;

  /**
   * Constructs a new exception with the specified message.
   *
   * @param errorCode customizable error code. Should be in range [0x00000301-0xFFFFFFFE]
   * @param message the message
   * @throws IllegalArgumentException if {@code errorCode} is out of allowed range
   */
  public CustomRSocketException(int errorCode, String message) {
    this(errorCode, message, null);
  }

  /**
   * Constructs a new exception with the specified message and cause.
   *
   * @param errorCode customizable error code. Should be in range [0x00000301-0xFFFFFFFE]
   * @param message the message
   * @param cause the cause of this exception
   * @throws IllegalArgumentException if {@code errorCode} is out of allowed range
   */
  public CustomRSocketException(int errorCode, String message, @Nullable Throwable cause) {
    super(errorCode, message, cause);
    if (errorCode > ErrorFrameCodec.MAX_USER_ALLOWED_ERROR_CODE
        && errorCode < ErrorFrameCodec.MIN_USER_ALLOWED_ERROR_CODE) {
      throw new IllegalArgumentException(
          "Allowed errorCode value should be in range [0x00000301-0xFFFFFFFE]", this);
    }
  }
}
