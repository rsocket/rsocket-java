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

package io.reactivesocket.mimetypes.internal;

import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * An {@link OutputStream} backed by a {@link ByteBuffer} which must only be used within the thread that retrieved it
 * via {@link #wrap(ByteBuffer)} method.
 * <p>
 * This code is copied from {@link com.fasterxml.jackson.databind.util.ByteBufferBackedOutputStream} with the
 * modifications to make the stream mutable per thread.
 */
final class ByteBufferOutputStream extends OutputStream {

    private ByteBuffer _b;

    public ByteBufferOutputStream() {
    }

    public ByteBufferOutputStream(ByteBuffer _b) {
        this._b = _b;
    }

    public ByteBufferOutputStream wrap(ByteBuffer buffer) {
        _b = buffer;
        return this;
    }

    @Override
    public void write(int b) {
        _b.put((byte) b);
    }

    @Override
    public void write(byte[] bytes, int off, int len) {
        _b.put(bytes, off, len);
    }
}