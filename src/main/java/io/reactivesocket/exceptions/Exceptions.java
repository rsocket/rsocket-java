/**
 * Copyright 2015 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.reactivesocket.exceptions;

import io.reactivesocket.Frame;

import java.nio.ByteBuffer;

import static io.reactivesocket.internal.frame.ErrorFrameFlyweight.*;
import static java.nio.charset.StandardCharsets.UTF_8;

public class Exceptions {
    public static Throwable from(Frame frame) {
        final int errorCode = Frame.Error.errorCode(frame);
        String message = "<empty message>";
        final ByteBuffer byteBuffer = frame.getMetadata();
        if (byteBuffer.hasArray()) {
            message = new String(byteBuffer.array(), UTF_8);
        }

        Throwable ex;
        switch (errorCode) {
            case APPLICATION_ERROR:
                ex = new ApplicationException(message);
                break;
            case CONNECTION_ERROR:
                ex = new ConnectionException(message);
                break;
            case INVALID:
                ex = new InvalidRequestException(message);
                break;
            case INVALID_SETUP:
                ex = new InvalidSetupException(message);
                break;
            case REJECTED:
                ex = new RejectedException(message);
                break;
            case REJECTED_SETUP:
                ex = new RejectedSetupException(message);
                break;
            case UNSUPPORTED_SETUP:
                ex = new UnsupportedSetupException(message);
                break;
            default:
                ex = new InvalidRequestException("Invalid Error frame");
        }
        return ex;
    }
}
