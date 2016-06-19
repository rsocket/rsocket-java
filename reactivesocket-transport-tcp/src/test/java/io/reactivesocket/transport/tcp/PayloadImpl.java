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

package io.reactivesocket.transport.tcp;

import io.reactivesocket.Frame;
import io.reactivesocket.Payload;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class PayloadImpl implements Payload {

    private final ByteBuffer data;

    public PayloadImpl(String data) {
        this.data = ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8));
    }

    public PayloadImpl(String data, Charset charset) {
        this.data = ByteBuffer.wrap(data.getBytes(charset));
    }

    public PayloadImpl(byte[] data) {
        this.data = ByteBuffer.wrap(data);
    }

    public PayloadImpl(ByteBuffer data) {
        this.data = data;
    }

    @Override
    public ByteBuffer getData() {
        return data;
    }

    @Override
    public ByteBuffer getMetadata() {
        return Frame.NULL_BYTEBUFFER;
    }
}
