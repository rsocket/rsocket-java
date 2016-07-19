package io.reactivesocket.tckdrivers.test;

import io.reactivesocket.Payload;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class PayloadImpl implements Payload // some JDK shoutout
{
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

    public static ByteBuffer byteBufferFromUtf8String(final String data)
    {
        final byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.wrap(bytes);
    }

    public boolean equals(Object obj)
    {
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
}
