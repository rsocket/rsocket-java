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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.reactivesocket.Frame;
import io.reactivesocket.Payload;
import io.reactivesocket.util.PayloadImpl;

import java.nio.ByteBuffer;

/**
 * Builder for appending buffers that grows dataCapacity as necessary. Similar to Aeron's PayloadBuilder.
 */
public class PayloadBuilder {
    public static final int INITIAL_CAPACITY = Math.max(Frame.DATA_MTU, Frame.METADATA_MTU);

    private final ByteBuf dataBuffer;
    private final ByteBuf metadataBuffer;

    public PayloadBuilder() {
        dataBuffer = Unpooled.directBuffer(INITIAL_CAPACITY);
        metadataBuffer = Unpooled.directBuffer(INITIAL_CAPACITY);
    }

    public Payload payload() {
        final ByteBuffer data;
        final ByteBuffer metadata;
        if (dataBuffer.readableBytes() > 0) {
            data = ByteBuffer.allocateDirect(dataBuffer.readableBytes());
            dataBuffer.readBytes(data);
            data.flip();
        } else {
            data = Frame.NULL_BYTEBUFFER;
        }
        if (metadataBuffer.readableBytes() > 0) {
            metadata = ByteBuffer.allocateDirect(metadataBuffer.readableBytes());
            metadataBuffer.readBytes(metadata);
            metadata.flip();
        } else {
            metadata = Frame.NULL_BYTEBUFFER;
        }
        return new PayloadImpl(data, metadata);
    }

    public void append(final Frame frame) {
        final ByteBuf data = FrameHeaderFlyweight.sliceFrameData(frame.content());
        final ByteBuf metadata = FrameHeaderFlyweight.sliceFrameMetadata(frame.content());

        dataBuffer.ensureWritable(data.readableBytes(), true);
        metadataBuffer.ensureWritable(metadata.readableBytes(), true);

        dataBuffer.writeBytes(data);
        metadataBuffer.writeBytes(metadata);
    }
}
