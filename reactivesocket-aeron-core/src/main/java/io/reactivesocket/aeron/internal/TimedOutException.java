package io.reactivesocket.aeron.internal;

public class TimedOutException extends RuntimeException {

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
