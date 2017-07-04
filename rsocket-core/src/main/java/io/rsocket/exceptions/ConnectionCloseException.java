package io.rsocket.exceptions;

import io.rsocket.frame.ErrorFrameFlyweight;

public class ConnectionCloseException extends RSocketException {
    public ConnectionCloseException(String message) {
        super(message);
    }

    public ConnectionCloseException(String message, Throwable cause) {
        super(message, cause);
    }

    @Override
    public int errorCode() {
        return ErrorFrameFlyweight.CONNECTION_CLOSE;
    }
}
