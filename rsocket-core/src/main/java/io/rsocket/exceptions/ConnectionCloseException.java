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

import io.rsocket.frame.ErrorFrameFlyweight;

/**
 * The connection is being terminated. Sender or Receiver of this frame MUST wait for outstanding
 * streams to terminate before closing the connection. New requests MAY not be accepted.
 *
 * @see <a href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#error-codes">Error
 *     Codes</a>
 */
public final class ConnectionCloseException extends RSocketException {

  private static final long serialVersionUID = -2214953527482377471L;

  /**
   * Constructs a new exception with the specified message.
   *
   * @param message the message
   * @throws NullPointerException if {@code message} is {@code null}
   */
  public ConnectionCloseException(String message) {
    super(message);
  }

  /**
   * Constructs a new exception with the specified message and cause.
   *
   * @param message the message
   * @param cause the cause of this exception
   * @throws NullPointerException if {@code message} or {@code cause} is {@code null}
   */
  public ConnectionCloseException(String message, Throwable cause) {
    super(message, cause);
  }

  @Override
  public int errorCode() {
    return ErrorFrameFlyweight.CONNECTION_CLOSE;
  }
}
