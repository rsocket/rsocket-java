/*
 * Copyright 2016 Facebook, Inc.
 * <p>
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *  <p>
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  <p>
 *  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations under the License.
 */

package io.reactivesocket.tckdrivers.common;

import io.reactivesocket.Payload;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class PayloadImpl implements Payload {
    private ByteBuffer data;
    private ByteBuffer metadata;

    public PayloadImpl(final String data, final String metadata) {
        if (null == data) {
            this.data = ByteBuffer.allocate(0);
        } else {
            this.data = byteBufferFromUtf8String(data);
        }

        if (null == metadata) {
            this.metadata = ByteBuffer.allocate(0);
        } else {
            this.metadata = byteBufferFromUtf8String(metadata);
        }
    }

    public static ByteBuffer byteBufferFromUtf8String(final String data) {
        final byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.wrap(bytes);
    }

    public boolean equals(Object obj) {
        System.out.println("equals: " + obj);
        final Payload rhs = (Payload) obj;

        return (data.equals(rhs.getData())) &&
                (metadata.equals(rhs.getMetadata()));
    }

    public ByteBuffer getData() {
        return data;
    }

    public ByteBuffer getMetadata() {
        return metadata;
    }

    // Converts a bytebuffer to a string
    public static String byteToString (ByteBuffer bt) {
        if (bt.hasArray()) {
            return new String(bt.array(),
                    bt.arrayOffset() + bt.position(),
                    bt.remaining());
        } else {
            final byte[] b = new byte[bt.remaining()];
            bt.duplicate().get(b);
            return new String(b);
        }
    }
}
