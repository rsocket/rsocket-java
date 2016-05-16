package io.reactivesocket.mimetypes.internal;

import io.reactivesocket.mimetypes.KVMetadata;
import io.reactivesocket.mimetypes.internal.cbor.CborCodec;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.nio.ByteBuffer;

public class ReactiveSocketDefaultMetadataCodec implements Codec {

    private final CborCodec cborCodec;

    private ReactiveSocketDefaultMetadataCodec(CborCodec cborCodec) {
        this.cborCodec = cborCodec;
    }

    public KVMetadata decodeDefault(ByteBuffer buffer) {
        return decode(buffer, KVMetadataImpl.class);
    }

    public KVMetadata decodeDefault(DirectBuffer buffer) {
        return decode(buffer, KVMetadataImpl.class);
    }

    @Override
    public <T> T decode(ByteBuffer buffer, Class<T> tClass) {
        return cborCodec.decode(buffer, tClass);
    }

    @Override
    public <T> T decode(DirectBuffer buffer, Class<T> tClass) {
        return cborCodec.decode(buffer, tClass);
    }

    @Override
    public <T> ByteBuffer encode(T toEncode) {
        return cborCodec.encode(toEncode);
    }

    @Override
    public <T> DirectBuffer encodeDirect(T toEncode) {
        return cborCodec.encodeDirect(toEncode);
    }

    @Override
    public <T> void encodeTo(ByteBuffer buffer, T toEncode) {
        cborCodec.encodeTo(buffer, toEncode);
    }

    @Override
    public <T> void encodeTo(MutableDirectBuffer buffer, T toEncode) {
        cborCodec.encodeTo(buffer, toEncode);
    }

    public static ReactiveSocketDefaultMetadataCodec create() {
        return new ReactiveSocketDefaultMetadataCodec(CborCodec.create());
    }

    public static ReactiveSocketDefaultMetadataCodec create(CborCodec cborCodec) {
        return new ReactiveSocketDefaultMetadataCodec(cborCodec);
    }
}
