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

import io.reactivesocket.Payload;

import java.nio.ByteBuffer;

public class ApplicationException extends RuntimeException {

    private static final long serialVersionUID = -8801579369150844447L;
    private final Payload payload;

    public ApplicationException(Payload payload) {
        this.payload = payload;
    }

    public ByteBuffer getErrorMetadata() {
        return payload.getMetadata();
    }

    public ByteBuffer getErrorData() {
        return payload.getData();
    }

    //@Override
    //public synchronized Throwable fillInStackTrace() {
    //    return this;
    //}
}
