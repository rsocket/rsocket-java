/**
 * Copyright 2015 Netflix, Inc.
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
package io.reactivesocket.internal.frame;

import io.reactivesocket.Frame;
import io.reactivesocket.FrameType;
import io.reactivesocket.Payload;

import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * Stateful iterator that can be re-used.
 *
 * Not thread-safe
 */
public class PayloadFragmenter implements Iterable<Frame>, Iterator<Frame>
{
    private enum Type
    {
        RESPONSE, RESPONSE_COMPLETE, REQUEST_CHANNEL
    }

    private final int metadataMtu;
    private final int dataMtu;
    private ByteBuffer metadata;
    private ByteBuffer data;
    private Type type;
    private int metadataOffset;
    private int dataOffset;
    private int streamId;
    private int initialRequestN;

    public PayloadFragmenter(final int metadataMtu, final int dataMtu)
    {
        this.metadataMtu = metadataMtu;
        this.dataMtu = dataMtu;
    }

    public void resetForResponse(final int streamId, final Payload payload)
    {
        reset(streamId, payload);
        type = Type.RESPONSE;
    }

    public void resetForResponseComplete(final int streamId, final Payload payload)
    {
        reset(streamId, payload);
        type = Type.RESPONSE_COMPLETE;
    }

    public void resetForRequestChannel(final int streamId, final Payload payload, final int initialRequestN)
    {
        reset(streamId, payload);
        type = Type.REQUEST_CHANNEL;
        this.initialRequestN = initialRequestN;
    }

    public static boolean requiresFragmenting(final int metadataMtu, final int dataMtu, final Payload payload)
    {
        final ByteBuffer metadata = payload.getMetadata();
        final ByteBuffer data = payload.getData();

        return metadata.remaining() > metadataMtu || data.remaining() > dataMtu;
    }

    public Iterator<Frame> iterator()
    {
        return this;
    }

    public boolean hasNext()
    {
        return dataOffset < data.capacity() || metadataOffset < metadata.remaining();
    }

    public Frame next()
    {
        final int metadataLength = Math.min(metadataMtu, metadata.remaining() - metadataOffset);
        final int dataLength = Math.min(dataMtu, data.remaining() - dataOffset);

        Frame result = null;

        final ByteBuffer metadataBuffer = metadataLength > 0 ?
            ByteBufferUtil.preservingSlice(metadata, metadataOffset, metadataOffset + metadataLength) : Frame.NULL_BYTEBUFFER;

        final ByteBuffer dataBuffer = dataLength > 0 ?
            ByteBufferUtil.preservingSlice(data, dataOffset, dataOffset + dataLength) : Frame.NULL_BYTEBUFFER;

        metadataOffset += metadataLength;
        dataOffset += dataLength;

        final boolean isMoreFollowing = metadataOffset < metadata.remaining() || dataOffset < data.remaining();
        int flags = 0;

        if (Type.RESPONSE == type)
        {
            if (isMoreFollowing)
            {
                flags |= FrameHeaderFlyweight.FLAGS_RESPONSE_F;
            }

            result = Frame.Response.from(streamId, FrameType.NEXT, metadataBuffer, dataBuffer, flags);
        }
        if (Type.RESPONSE_COMPLETE == type)
        {
            if (isMoreFollowing)
            {
                flags |= FrameHeaderFlyweight.FLAGS_RESPONSE_F;
            }

            result = Frame.Response.from(streamId, FrameType.NEXT_COMPLETE, metadataBuffer, dataBuffer, flags);
        }
        else if (Type.REQUEST_CHANNEL == type)
        {
            if (isMoreFollowing)
            {
                flags |= FrameHeaderFlyweight.FLAGS_REQUEST_CHANNEL_F;
            }

            result = Frame.Request.from(streamId, FrameType.REQUEST_CHANNEL, metadataBuffer, dataBuffer, initialRequestN, flags);
            initialRequestN = 0;
        }

        return result;
    }

    private void reset(final int streamId, final Payload payload)
    {
        data = payload.getData();
        metadata = payload.getMetadata();
        metadataOffset = 0;
        dataOffset = 0;
        this.streamId = streamId;
    }
}
