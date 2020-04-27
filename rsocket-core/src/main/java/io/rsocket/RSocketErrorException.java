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

package io.rsocket;

import reactor.util.annotation.Nullable;

/**
 * Exception that represents an RSocket protocol error.
 *
 * @see <a href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#error-frame-0x0b">ERROR
 *     Frame (0x0B)</a>
 */
public class RSocketErrorException extends RuntimeException {

  private static final long serialVersionUID = -1628781753426267554L;

  private static final int MIN_ERROR_CODE = 0x00000001;

  private static final int MAX_ERROR_CODE = 0xFFFFFFFE;

  private final int errorCode;

  /**
   * Constructor with a protocol error code and a message.
   *
   * @param errorCode the RSocket protocol error code
   * @param message error explanation
   */
  public RSocketErrorException(int errorCode, String message) {
    this(errorCode, message, null);
  }

  /**
   * Alternative to {@link #RSocketErrorException(int, String)} with a root cause.
   *
   * @param errorCode the RSocket protocol error code
   * @param message error explanation
   * @param cause a root cause for the error
   */
  public RSocketErrorException(int errorCode, String message, @Nullable Throwable cause) {
    super(message, cause);
    this.errorCode = errorCode;
    if (errorCode > MAX_ERROR_CODE && errorCode < MIN_ERROR_CODE) {
      throw new IllegalArgumentException(
          "Allowed errorCode value should be in range [0x00000001-0xFFFFFFFE]", this);
    }
  }

  /**
   * Return the RSocket <a
   * href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#error-codes">error code</a>
   * represented by this exception
   *
   * @return the RSocket protocol error code
   */
  public int errorCode() {
    return errorCode;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()
        + " (0x"
        + Integer.toHexString(errorCode)
        + "): "
        + getMessage();
  }
}
