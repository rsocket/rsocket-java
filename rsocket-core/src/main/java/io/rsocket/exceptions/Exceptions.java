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
package io.rsocket.exceptions;

import io.rsocket.Frame;
import io.rsocket.util.PayloadImpl;

import java.nio.charset.StandardCharsets;

import static io.rsocket.frame.ErrorFrameFlyweight.*;

public class Exceptions {

    private Exceptions() {}

    public static RuntimeException from(Frame frame) {
        final int errorCode = Frame.Error.errorCode(frame);

        RuntimeException ex;
        switch (errorCode) {
            case APPLICATION_ERROR:
                ex = new ApplicationException(new PayloadImpl(frame));
                break;
            case CONNECTION_ERROR:
                ex = new ConnectionException(StandardCharsets.UTF_8.decode(frame.getData()).toString());
                break;
            case INVALID:
                ex = new InvalidRequestException(StandardCharsets.UTF_8.decode(frame.getData()).toString());
                break;
            case INVALID_SETUP:
                ex = new InvalidSetupException(StandardCharsets.UTF_8.decode(frame.getData()).toString());
                break;
            case REJECTED:
                ex = new RejectedException(StandardCharsets.UTF_8.decode(frame.getData()).toString());
                break;
            case REJECTED_SETUP:
                ex = new RejectedSetupException(StandardCharsets.UTF_8.decode(frame.getData()).toString());
                break;
            case UNSUPPORTED_SETUP:
                ex = new UnsupportedSetupException(StandardCharsets.UTF_8.decode(frame.getData()).toString());
                break;
            default:
                ex = new InvalidRequestException("Invalid Error frame");
        }
        return ex;
    }
}
