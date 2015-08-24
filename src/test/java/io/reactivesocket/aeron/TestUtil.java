package io.reactivesocket.aeron;

import io.reactivesocket.Frame;
import io.reactivesocket.FrameType;
import io.reactivesocket.Payload;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class TestUtil
{
    public static Frame utf8EncodedFrame(final long streamId, final FrameType type, final String data)
    {
        return Frame.from(streamId, type, byteBufferFromUtf8String(data));
    }

    public static Payload utf8EncodedPayload(final String data, final String metadata)
    {
        return new PayloadImpl(data, metadata);
    }

    public static String byteToString(final ByteBuffer byteBuffer)
    {
        final byte[] bytes = new byte[byteBuffer.capacity()];
        byteBuffer.get(bytes);
        return new String(bytes, Charset.forName("UTF-8"));
    }

    public static ByteBuffer byteBufferFromUtf8String(final String data)
    {
        final byte[] bytes = data.getBytes(Charset.forName("UTF-8"));
        return ByteBuffer.wrap(bytes);
    }

    private static class PayloadImpl implements Payload // some JDK shoutout
    {
        private ByteBuffer data;
        private ByteBuffer metadata;

        public PayloadImpl(final String data, final String metadata)
        {
            if (null == data)
            {
                this.data = ByteBuffer.allocate(0);
            }
            else
            {
                this.data = byteBufferFromUtf8String(data);
            }

            if (null == metadata)
            {
                this.metadata = ByteBuffer.allocate(0);
            }
            else
            {
                this.metadata = byteBufferFromUtf8String(metadata);
            }
        }

        public boolean equals(Object obj)
        {
            final Payload rhs = (Payload)obj;

            return (TestUtil.byteToString(data).equals(TestUtil.byteToString(rhs.getData()))) &&
                (TestUtil.byteToString(metadata).equals(TestUtil.byteToString(rhs.getMetadata())));
        }

        public ByteBuffer getData()
        {
            return data;
        }

        public ByteBuffer getMetadata()
        {
            return metadata;
        }
    }

}