/*
 * Copyright 2016 Netflix, Inc.
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
import io.reactivesocket.frame.ByteBufferUtil;

import java.nio.ByteBuffer;

import static io.reactivesocket.frame.ErrorFrameFlyweight.*;

public class Exceptions {

    private Exceptions() {}

    public static RuntimeException from(Frame frame) {
        final int errorCode = Frame.Error.errorCode(frame);
        ByteBuffer dataBuffer = frame.getData();
        String message = dataBuffer.remaining() == 0 ? "<empty message>" : ByteBufferUtil.toUtf8String(dataBuffer);

        RuntimeException ex;
        switch (errorCode) {
            case APPLICATION_ERROR:
                ex = new ApplicationException(frame);
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
