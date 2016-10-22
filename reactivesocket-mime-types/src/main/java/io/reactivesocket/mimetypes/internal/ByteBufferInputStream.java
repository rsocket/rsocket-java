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

import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * This code is copied from {@link com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream} with the
 * modifications to make the stream mutable per thread.
 */
final class ByteBufferInputStream extends InputStream {

    private ByteBuffer _b;

    public ByteBufferInputStream() {
    }

    public ByteBufferInputStream(ByteBuffer _b) {
        this._b = _b;
    }

    /**
     * Returns a {@link ThreadLocal} instance of the {@link InputStream} which wraps the passed buffer.
     *
     * <b>This instance must not leak from the calling thread.</b>
     *
     * @param buffer Buffer to wrap.
     */
    public ByteBufferInputStream wrap(ByteBuffer buffer) {
        _b = buffer;
        return this;
    }

    @Override
    public int available() {
        return _b.remaining();
    }

    @Override
    public int read() {
        return _b.hasRemaining() ? _b.get() & 0xFF : -1;
    }

    @Override
    public int read(byte[] bytes, int off, int len) {
        if (!_b.hasRemaining()) {
            return -1;
        }
        len = Math.min(len, _b.remaining());
        _b.get(bytes, off, len);
        return len;
    }
}