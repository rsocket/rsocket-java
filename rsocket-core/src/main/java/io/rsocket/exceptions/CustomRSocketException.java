package io.rsocket.exceptions;

import io.rsocket.frame.ErrorType;

public class CustomRSocketException extends RSocketException {
  private static final long serialVersionUID = 7873267740343446585L;

  private final int errorCode;

  /**
   * Constructs a new exception with the specified message.
   *
   * @param errorCode customizable error code. Should be in range [0x00000301-0xFFFFFFFE]
   * @param message the message
   * @throws NullPointerException if {@code message} is {@code null}
   * @throws IllegalArgumentException if {@code errorCode} is out of allowed range
   */
  public CustomRSocketException(int errorCode, String message) {
    super(message);
    if (errorCode > ErrorType.MAX_USER_ALLOWED_ERROR_CODE
        && errorCode < ErrorType.MIN_USER_ALLOWED_ERROR_CODE) {
      throw new IllegalArgumentException(
          "Allowed errorCode value should be in range [0x00000301-0xFFFFFFFE]");
    }
    this.errorCode = errorCode;
  }

  /**
   * Constructs a new exception with the specified message and cause.
   *
   * @param errorCode customizable error code. Should be in range [0x00000301-0xFFFFFFFE]
   * @param message the message
   * @param cause the cause of this exception
   * @throws NullPointerException if {@code message} or {@code cause} is {@code null}
   * @throws IllegalArgumentException if {@code errorCode} is out of allowed range
   */
  public CustomRSocketException(int errorCode, String message, Throwable cause) {
    super(message, cause);
    if (errorCode > ErrorType.MAX_USER_ALLOWED_ERROR_CODE
        && errorCode < ErrorType.MIN_USER_ALLOWED_ERROR_CODE) {
      throw new IllegalArgumentException(
          "Allowed errorCode value should be in range [0x00000301-0xFFFFFFFE]");
    }
    this.errorCode = errorCode;
  }

  @Override
  public int errorCode() {
    return errorCode;
  }
}
