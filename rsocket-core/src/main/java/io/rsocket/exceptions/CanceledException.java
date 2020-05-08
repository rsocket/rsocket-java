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

/**
 * The Responder canceled the request but may have started processing it (similar to REJECTED but
 * doesn't guarantee lack of side-effects).
 *
 * @see <a href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#error-codes">Error
 *     Codes</a>
 */
public final class CanceledException extends RSocketException {

  private static final long serialVersionUID = 5074789326089722770L;

  /**
   * Constructs a new exception with the specified message.
   *
   * @param message the message
   */
  public CanceledException(String message) {
    this(message, null);
  }

  /**
   * Constructs a new exception with the specified message and cause.
   *
   * @param message the message
   * @param cause the cause of this exception
   */
  public CanceledException(String message, @Nullable Throwable cause) {
    super(ErrorFrameCodec.CANCELED, message, cause);
  }
}
