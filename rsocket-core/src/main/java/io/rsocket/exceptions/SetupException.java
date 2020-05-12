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

/** The root of the setup exception hierarchy. */
public abstract class SetupException extends RSocketException {

  private static final long serialVersionUID = -2928269501877732756L;

  /**
   * Constructs a new exception with the specified message.
   *
   * @param message the message
   * @deprecated please use {@link #SetupException(int, String, Throwable)}
   */
  @Deprecated
  public SetupException(String message) {
    this(message, null);
  }

  /**
   * Constructs a new exception with the specified message and cause.
   *
   * @param message the message
   * @param cause the cause of this exception
   * @deprecated please use {@link #SetupException(int, String, Throwable)}
   */
  @Deprecated
  public SetupException(String message, @Nullable Throwable cause) {
    this(ErrorFrameCodec.INVALID_SETUP, message, cause);
  }

  /**
   * Constructs a new exception with the specified error code, message and cause.
   *
   * @param errorCode the RSocket protocol code
   * @param message the message
   * @param cause the cause of this exception
   */
  public SetupException(int errorCode, String message, @Nullable Throwable cause) {
    super(errorCode, message, cause);
  }
}
