package io.rsocket.exceptions;

public abstract class RSocketException extends RuntimeException {
    public RSocketException(String message) {
        super(message);
    }

    public RSocketException(String message, Throwable cause) {
        super(message, cause);
    }

    public abstract int errorCode();
}
