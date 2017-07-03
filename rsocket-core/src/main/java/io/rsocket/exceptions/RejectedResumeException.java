package io.rsocket.exceptions;

import io.rsocket.frame.ErrorFrameFlyweight;

public class RejectedResumeException extends RSocketException {
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
