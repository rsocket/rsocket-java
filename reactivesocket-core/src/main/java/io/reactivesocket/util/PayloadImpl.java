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

package io.reactivesocket.util;

import io.reactivesocket.Frame;
import io.reactivesocket.Payload;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * An implementation of {@link Payload}. This implementation is <b>not</b> thread-safe, and hence any method can not be
 * invoked concurrently.
 *
 * <h2>Reusability</h2>
 *
 * By default, an instance is reusable, i.e. everytime {@link #getData()} or {@link #getMetadata()} is invoked, the
 * position of the returned buffer is reset to the start of data in the buffer. For creating an instance for single-use,
 * {@link #PayloadImpl(ByteBuffer, ByteBuffer, boolean)} must be used.
 */
public class PayloadImpl implements Payload {

    public static final Payload EMPTY = new PayloadImpl(Frame.NULL_BYTEBUFFER, Frame.NULL_BYTEBUFFER);

    private final ByteBuffer data;
    private final int dataStartPosition;
    private final int metadataStartPosition;
    private final boolean reusable;
    private final ByteBuffer metadata;

    public PayloadImpl(String data) {
        this(data, (String) null);
    }

    public PayloadImpl(String data, String metadata) {
        this(fromString(data), fromString(metadata));
    }

    public PayloadImpl(String data, Charset charset) {
        this(fromString(data, charset), fromString(null));
    }

    public PayloadImpl(String data, Charset dataCharset, String metadata, Charset metaDataCharset) {
        this(fromString(data, dataCharset), fromString(metadata, metaDataCharset));
    }

    public PayloadImpl(byte[] data) {
        this(ByteBuffer.wrap(data), Frame.NULL_BYTEBUFFER);
    }

    public PayloadImpl(byte[] data, byte[] metadata) {
        this(ByteBuffer.wrap(data), ByteBuffer.wrap(metadata));
    }

    public PayloadImpl(ByteBuffer data) {
        this(data, Frame.NULL_BYTEBUFFER);
    }

    public PayloadImpl(ByteBuffer data, ByteBuffer metadata) {
        this(data, metadata, true);
    }

    /**
     * New instance where every invocation of {@link #getMetadata()} and {@link #getData()} will reset the position of
     * the buffer to the position when it is created, if and only if, {@code reusable} is set to {@code true}.
     *
     * @param data Buffer for data.
     * @param metadata Buffer for metadata.
     * @param reusable {@code true} if the buffer position is to be reset on every invocation of {@link #getData()} and
     * {@link #getMetadata()}.
     */
    public PayloadImpl(ByteBuffer data, ByteBuffer metadata, boolean reusable) {
        this.data = data;
        this.reusable = reusable;
        this.metadata = null == metadata ? Frame.NULL_BYTEBUFFER : metadata;
        dataStartPosition = reusable ? this.data.position() : 0;
        metadataStartPosition = reusable ? this.metadata.position() : 0;
    }

    @Override
    public ByteBuffer getData() {
        if (reusable) {
            data.position(dataStartPosition);
        }
        return data;
    }

    @Override
    public ByteBuffer getMetadata() {
        if (reusable) {
            metadata.position(metadataStartPosition);
        }
        return metadata;
    }

    private static ByteBuffer fromString(String data) {
        return fromString(data, Charset.defaultCharset());
    }

    private static ByteBuffer fromString(String data, Charset charset) {
        return data == null ? Frame.NULL_BYTEBUFFER : ByteBuffer.wrap(data.getBytes(charset));
    }
}
