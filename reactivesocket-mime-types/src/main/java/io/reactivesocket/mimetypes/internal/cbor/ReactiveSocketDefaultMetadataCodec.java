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

package io.reactivesocket.mimetypes.internal.cbor;

import io.reactivesocket.mimetypes.KVMetadata;
import io.reactivesocket.mimetypes.internal.Codec;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.nio.ByteBuffer;

public class ReactiveSocketDefaultMetadataCodec implements Codec {

    private final CborCodec cborCodec;

    private ReactiveSocketDefaultMetadataCodec(CborCodec cborCodec) {
        this.cborCodec = cborCodec;
    }

    public KVMetadata decodeDefault(ByteBuffer buffer) {
        return MetadataCodec.INSTANCE.decode(buffer, SlicedBufferKVMetadata.class);
    }

    public KVMetadata decodeDefault(DirectBuffer buffer, int offset) {
        return MetadataCodec.INSTANCE.decode(buffer, offset, SlicedBufferKVMetadata.class);
    }

    @Override
    public <T> T decode(ByteBuffer buffer, Class<T> tClass) {
        if (KVMetadata.class.isAssignableFrom(tClass)) {
            @SuppressWarnings("unchecked")
            T t = (T) decodeDefault(buffer);
            return t;
        }
        return cborCodec.decode(buffer, tClass);
    }

    @Override
    public <T> T decode(DirectBuffer buffer, int offset, Class<T> tClass) {
        if (KVMetadata.class.isAssignableFrom(tClass)) {
            @SuppressWarnings("unchecked")
            T t = (T) decodeDefault(buffer, offset);
            return t;
        }
        return cborCodec.decode(buffer, offset, tClass);
    }

    @Override
    public <T> ByteBuffer encode(T toEncode) {
        if (KVMetadata.class.isAssignableFrom(toEncode.getClass())) {
            return MetadataCodec.INSTANCE.encode(toEncode);
        }
        return cborCodec.encode(toEncode);
    }

    @Override
    public <T> DirectBuffer encodeDirect(T toEncode) {
        if (KVMetadata.class.isAssignableFrom(toEncode.getClass())) {
            return MetadataCodec.INSTANCE.encodeDirect(toEncode);
        }
        return cborCodec.encodeDirect(toEncode);
    }

    @Override
    public <T> void encodeTo(ByteBuffer buffer, T toEncode) {
        if (KVMetadata.class.isAssignableFrom(toEncode.getClass())) {
            MetadataCodec.INSTANCE.encodeTo(buffer, toEncode);
        } else {
            cborCodec.encodeTo(buffer, toEncode);
        }
    }

    @Override
    public <T> void encodeTo(MutableDirectBuffer buffer, T toEncode, int offset) {
        if (KVMetadata.class.isAssignableFrom(toEncode.getClass())) {
            MetadataCodec.INSTANCE.encodeTo(buffer, toEncode, offset);
        } else {
            cborCodec.encodeTo(buffer, toEncode, offset);
        }
    }

    public static ReactiveSocketDefaultMetadataCodec create() {
        return new ReactiveSocketDefaultMetadataCodec(CborCodec.create());
    }

    public static ReactiveSocketDefaultMetadataCodec create(CborCodec cborCodec) {
        return new ReactiveSocketDefaultMetadataCodec(cborCodec);
    }
}
