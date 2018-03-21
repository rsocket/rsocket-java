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

import java.util.Objects;
import reactor.util.annotation.Nullable;

/** The root of the RSocket exception hierarchy. */
public abstract class RSocketException extends RuntimeException {

  private static final long serialVersionUID = 2912815394105575423L;

  /**
   * Constructs a new exception with the specified message.
   *
   * @param message the message
   * @throws NullPointerException if {@code message} is {@code null}
   */
  public RSocketException(String message) {
    super(Objects.requireNonNull(message, "message must not be null"));
  }

  /**
   * Constructs a new exception with the specified message and cause.
   *
   * @param message the message
   * @param cause the cause of this exception
   * @throws NullPointerException if {@code message} is {@code null}
   */
  public RSocketException(String message, @Nullable Throwable cause) {
    super(Objects.requireNonNull(message, "message must not be null"), cause);
  }

  /**
   * Returns the RSocket <a
   * href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#error-codes">error code</a>
   * represented by this exception
   *
   * @return the RSocket error code
   */
  public abstract int errorCode();
}
