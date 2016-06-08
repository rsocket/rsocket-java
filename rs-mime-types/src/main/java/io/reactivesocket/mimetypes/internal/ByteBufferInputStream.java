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