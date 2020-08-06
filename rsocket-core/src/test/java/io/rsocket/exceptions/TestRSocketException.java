package io.rsocket.exceptions;

import io.rsocket.RSocketErrorException;
import io.rsocket.frame.ErrorFrameCodec;

public class TestRSocketException extends RSocketErrorException {
  private static final long serialVersionUID = 7873267740343446585L;

  private final int errorCode;

  /**
   * Constructs a new exception with the specified message.
   *
   * @param errorCode customizable error code
   * @param message the message
   * @throws NullPointerException if {@code message} is {@code null}
   * @throws IllegalArgumentException if {@code errorCode} is out of allowed range
   */
  public TestRSocketException(int errorCode, String message) {
    super(ErrorFrameCodec.APPLICATION_ERROR, message);
    this.errorCode = errorCode;
  }

  /**
   * Constructs a new exception with the specified message and cause.
   *
   * @param errorCode customizable error code
   * @param message the message
   * @param cause the cause of this exception
   * @throws NullPointerException if {@code message} or {@code cause} is {@code null}
   * @throws IllegalArgumentException if {@code errorCode} is out of allowed range
   */
  public TestRSocketException(int errorCode, String message, Throwable cause) {
    super(ErrorFrameCodec.APPLICATION_ERROR, message, cause);
    this.errorCode = errorCode;
  }

  @Override
  public int errorCode() {
    return errorCode;
  }
}
