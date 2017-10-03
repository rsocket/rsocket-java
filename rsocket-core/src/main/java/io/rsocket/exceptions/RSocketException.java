package io.rsocket.exceptions;

public abstract class RSocketException extends RuntimeException {

  private static final long serialVersionUID = 2912815394105575423L;

  public RSocketException(String message) {
    super(message);
  }

  public RSocketException(String message, Throwable cause) {
    super(message, cause);
  }

  public abstract int errorCode();
}
