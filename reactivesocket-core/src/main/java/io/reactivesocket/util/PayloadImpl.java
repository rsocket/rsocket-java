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
 * An implementation of {@link Payload}
 */
public class PayloadImpl implements Payload {

    public static final Payload EMPTY = new PayloadImpl(Frame.NULL_BYTEBUFFER, Frame.NULL_BYTEBUFFER);

    private final ByteBuffer data;
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
        this.data = data;
        this.metadata = null == metadata ? Frame.NULL_BYTEBUFFER : metadata;
    }

    @Override
    public ByteBuffer getData() {
        return data;
    }

    @Override
    public ByteBuffer getMetadata() {
        return metadata;
    }

    private static ByteBuffer fromString(String data) {
        return fromString(data, Charset.defaultCharset());
    }

    private static ByteBuffer fromString(String data, Charset charset) {
        return data == null ? Frame.NULL_BYTEBUFFER : ByteBuffer.wrap(data.getBytes(charset));
    }
}
