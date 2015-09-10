package io.reactivesocket.aeron;

import io.reactivesocket.Frame;
import io.reactivesocket.FrameType;
import io.reactivesocket.Payload;
import uk.co.real_logic.agrona.MutableDirectBuffer;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class TestUtil
{
    public static Frame utf8EncodedRequestFrame(final int streamId, final FrameType type, final String data, final int initialRequestN)
    {
        return Frame.Request.from(streamId, type, new Payload()
        {
            public ByteBuffer getData()
            {
                return byteBufferFromUtf8String(data);
            }

            public ByteBuffer getMetadata()
            {
                return Frame.NULL_BYTEBUFFER;
            }
        }, initialRequestN);
    }

    public static Frame utf8EncodedResponseFrame(final int streamId, final FrameType type, final String data)
    {
        return Frame.Response.from(streamId, type, utf8EncodedPayload(data, null));
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
        final byte[] bytes = new byte[byteBuffer.capacity()];
        byteBuffer.get(bytes);
        return new String(bytes, Charset.forName("UTF-8"));
    }

    public static ByteBuffer byteBufferFromUtf8String(final String data)
    {
        final byte[] bytes = data.getBytes(Charset.forName("UTF-8"));
        return ByteBuffer.wrap(bytes);
    }

    public static void copyFrame(final MutableDirectBuffer dst, final int offset, final Frame frame)
    {
        dst.putBytes(offset, frame.getByteBuffer(), frame.offset(), frame.length());
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
            System.out.println("equals: " + obj);
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