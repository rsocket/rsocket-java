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
package io.reactivesocket;

import io.reactivesocket.util.PayloadImpl;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class TestUtil
{
    private static final char[] hexArray = "0123456789ABCDEF".toCharArray();

    private TestUtil() {}

    public static Frame utf8EncodedRequestFrame(final int streamId, final FrameType type, final String data, final int initialRequestN)
    {
        return Frame.Request.from(streamId, type, new PayloadImpl(data), initialRequestN);
    }

    public static Frame utf8EncodedResponseFrame(final int streamId, final FrameType type, final String data)
    {
        return Frame.PayloadFrame.from(streamId, type, new PayloadImpl(data));
    }

    public static Frame utf8EncodedErrorFrame(final int streamId, final String data)
    {
        return Frame.Error.from(streamId, new Exception(data));
    }

    public static Payload utf8EncodedPayload(final String data, final String metadata)
    {
        return new PayloadImpl(data, metadata);
    }

    public static String byteToString(final ByteBuffer byteBuffer)
    {
        return StandardCharsets.UTF_8.decode(byteBuffer).toString();
    }

    public static ByteBuffer byteBufferFromUtf8String(final String data)
    {
        return StandardCharsets.UTF_8.encode(data);
    }
}
