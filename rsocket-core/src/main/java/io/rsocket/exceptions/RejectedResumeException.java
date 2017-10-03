package io.rsocket.exceptions;

import io.rsocket.frame.ErrorFrameFlyweight;

public class RejectedResumeException extends RSocketException {

  private static final long serialVersionUID = 6953301234450438491L;

  public RejectedResumeException(String message) {
    super(message);
  }

  public RejectedResumeException(String message, Throwable cause) {
    super(message, cause);
  }

  @Override
  public int errorCode() {
    return ErrorFrameFlyweight.REJECTED_RESUME;
  }
}
