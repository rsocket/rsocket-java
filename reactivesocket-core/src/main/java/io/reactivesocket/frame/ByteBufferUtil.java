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
package io.reactivesocket.frame;

import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ByteBufferUtil {

    private ByteBufferUtil() {}

    /**
     * Slice a portion of the {@link ByteBuffer} while preserving the buffers position and limit.
     *
     * NOTE: Missing functionality from {@link ByteBuffer}
     *
     * @param byteBuffer to slice off of
     * @param position   to start slice at
     * @param limit      to slice to
     * @return slice of byteBuffer with passed ByteBuffer preserved position and limit.
     */
    public static ByteBuffer preservingSlice(final ByteBuffer byteBuffer, final int position, final int limit) {
        final int savedPosition = byteBuffer.position();
        final int savedLimit = byteBuffer.limit();

        byteBuffer.limit(limit).position(position);

        final ByteBuffer result = byteBuffer.slice();

        byteBuffer.limit(savedLimit).position(savedPosition);
        return result;
    }

    public static String toUtf8String(ByteBuffer byteBuffer) {
        byte[] bytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytes);
        return new String(bytes, UTF_8);
    }
}
