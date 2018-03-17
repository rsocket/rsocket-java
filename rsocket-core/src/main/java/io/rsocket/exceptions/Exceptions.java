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

import static io.rsocket.frame.ErrorFrameFlyweight.*;

import io.rsocket.Frame;

public class Exceptions {

  private Exceptions() {}

  public static RuntimeException from(Frame frame) {
    final int errorCode = Frame.Error.errorCode(frame);

    String message = frame.getDataUtf8();
    switch (errorCode) {
      case APPLICATION_ERROR:
        return new ApplicationException(message);
      case CANCELED:
        return new CancelException(message);
      case CONNECTION_CLOSE:
        return new ConnectionCloseException(message);
      case CONNECTION_ERROR:
        return new ConnectionException(message);
      case INVALID:
        return new InvalidRequestException(message);
      case INVALID_SETUP:
        return new InvalidSetupException(message);
      case REJECTED:
        return new RejectedException(message);
      case REJECTED_RESUME:
        return new RejectedResumeException(message);
      case REJECTED_SETUP:
        return new RejectedSetupException(message);
      case UNSUPPORTED_SETUP:
        return new UnsupportedSetupException(message);
      default:
        return new InvalidRequestException(
            "Invalid Error frame: " + errorCode + " '" + message + "'");
    }
  }
}
