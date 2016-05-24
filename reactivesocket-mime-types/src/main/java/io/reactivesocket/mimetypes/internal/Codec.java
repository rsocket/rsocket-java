package io.reactivesocket.mimetypes.internal;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.nio.ByteBuffer;

public interface Codec {

    <T> T decode(ByteBuffer buffer, Class<T> tClass);

    <T> T decode(DirectBuffer buffer, int offset, Class<T> tClass);

    <T> ByteBuffer encode(T toEncode);

    <T> DirectBuffer encodeDirect(T toEncode);

    <T> void encodeTo(ByteBuffer buffer, T toEncode);

    <T> void encodeTo(MutableDirectBuffer buffer, T toEncode, int offset);
}
