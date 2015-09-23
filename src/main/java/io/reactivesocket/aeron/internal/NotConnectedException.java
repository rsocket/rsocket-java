package io.reactivesocket.aeron.internal;

public class NotConnectedException extends RuntimeException {

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }

}
