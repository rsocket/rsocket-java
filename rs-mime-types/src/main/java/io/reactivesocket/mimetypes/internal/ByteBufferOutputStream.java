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