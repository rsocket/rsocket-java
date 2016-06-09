/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.reactivesocket.mimetypes.internal;

public class MalformedInputException extends RuntimeException {

    private static final long serialVersionUID = 3130502874275862715L;

    public MalformedInputException(String message) {
        super(message);
    }

    public MalformedInputException(String message, Throwable cause) {
        super(message, cause);
    }

    public MalformedInputException(Throwable cause) {
        super(cause);
    }

    public MalformedInputException(String message, Throwable cause, boolean enableSuppression,
                                   boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
