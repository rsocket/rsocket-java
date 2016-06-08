package io.reactivesocket.mimetypes;

import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.mimetypes.internal.Codec;
import io.reactivesocket.mimetypes.internal.cbor.CborCodec;
import io.reactivesocket.mimetypes.internal.cbor.ReactiveSocketDefaultMetadataCodec;
import io.reactivesocket.mimetypes.internal.json.JsonCodec;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.nio.ByteBuffer;
import java.util.EnumMap;

import static io.reactivesocket.mimetypes.SupportedMimeTypes.*;

/**
 * A factory to retrieve {@link MimeType} instances for {@link SupportedMimeTypes}. The retrieved mime type instances
 * are thread-safe.
 */
public final class MimeTypeFactory {

    private static final EnumMap<SupportedMimeTypes, Codec> codecs;
    private static final EnumMap<SupportedMimeTypes, EnumMap<SupportedMimeTypes, MimeType>> mimeTypes;

    static {
        codecs = new EnumMap<SupportedMimeTypes, Codec>(SupportedMimeTypes.class);
        codecs.put(CBOR, CborCodec.create());
        codecs.put(JSON, JsonCodec.create());
        codecs.put(ReactiveSocketDefaultMetadata, ReactiveSocketDefaultMetadataCodec.create());

        mimeTypes = new EnumMap<>(SupportedMimeTypes.class);
        mimeTypes.put(CBOR, getEnumMapForMetadataCodec(CBOR));
        mimeTypes.put(JSON, getEnumMapForMetadataCodec(JSON));
        mimeTypes.put(ReactiveSocketDefaultMetadata, getEnumMapForMetadataCodec(ReactiveSocketDefaultMetadata));
    }

    private MimeTypeFactory() {
    }

    /**
     * Provides an appropriate {@link MimeType} for the passed {@link ConnectionSetupPayload}.
     * Only the mime types represented by {@link SupportedMimeTypes} are supported by this factory. For any other mime
     * type, this method will throw an exception.
     * It is safer to first retrieve the mime types and then use {@link #from(SupportedMimeTypes, SupportedMimeTypes)}
     * method.
     *
     * @param setup Setup for which the mime type is to be fetched.
     *
     * @return Appropriate {@link MimeType} for the passed {@code setup}.
     *
     * @throws IllegalArgumentException If the mime type for either data or metadata is not supported by this factory.
     */
    public static MimeType from(ConnectionSetupPayload setup) {
        SupportedMimeTypes metaMimeType = parseOrDie(setup.metadataMimeType());
        SupportedMimeTypes dataMimeType = parseOrDie(setup.dataMimeType());

        return from(metaMimeType, dataMimeType);
    }

    /**
     * Same as calling {@code from(mimeType, mimeType}.
     *
     * @param mimeType Mime type to be used for both data and metadata.
     *
     * @return MimeType for the passed {@code mimeType}
     */
    public static MimeType from(SupportedMimeTypes mimeType) {
        return from(mimeType, mimeType);
    }

    /**
     * Provides an appropriate {@link MimeType} for the passed date and metadata mime types.
     *
     * @param metadataMimeType Mime type for metadata.
     * @param dataMimeType Mime type for data.
     *
     * @return Appropriate {@link MimeType} to use.
     */
    public static MimeType from(SupportedMimeTypes metadataMimeType, SupportedMimeTypes dataMimeType) {
        if (null == metadataMimeType) {
            throw new IllegalArgumentException("Metadata mime type can not be null.");
        }
        if (null == dataMimeType) {
            throw new IllegalArgumentException("Data mime type can not be null.");
        }

        return mimeTypes.get(metadataMimeType).get(dataMimeType);
    }

    private static EnumMap<SupportedMimeTypes, MimeType> getEnumMapForMetadataCodec(SupportedMimeTypes metaMime) {

        final Codec metaMimeCodec = codecs.get(metaMime);

        EnumMap<SupportedMimeTypes, MimeType> toReturn =
                new EnumMap<SupportedMimeTypes, MimeType>(SupportedMimeTypes.class);

        toReturn.put(CBOR, new MimeTypeImpl(metaMimeCodec, codecs.get(CBOR)));
        toReturn.put(JSON, new MimeTypeImpl(metaMimeCodec, codecs.get(JSON)));
        toReturn.put(ReactiveSocketDefaultMetadata,
                     new MimeTypeImpl(metaMimeCodec, codecs.get(ReactiveSocketDefaultMetadata)));

        return toReturn;
    }

    /*Visible for testing*/ Codec getCodec(SupportedMimeTypes mimeType) {
        return codecs.get(mimeType);
    }

    private static class MimeTypeImpl implements MimeType {

        private final Codec metaCodec;
        private final Codec dataCodec;

        public MimeTypeImpl(Codec metaCodec, Codec dataCodec) {
            this.metaCodec = metaCodec;
            this.dataCodec = dataCodec;
        }

        @Override
        public <T> T decodeMetadata(ByteBuffer toDecode, Class<T> clazz) {
            return metaCodec.decode(toDecode, clazz);
        }

        @Override
        public <T> T decodeMetadata(DirectBuffer toDecode, Class<T> clazz, int offset) {
            return metaCodec.decode(toDecode, offset, clazz);
        }

        @Override
        public <T> ByteBuffer encodeMetadata(T toEncode) {
            return metaCodec.encode(toEncode);
        }

        @Override
        public <T> DirectBuffer encodeMetadataDirect(T toEncode) {
            return metaCodec.encodeDirect(toEncode);
        }

        @Override
        public <T> void encodeMetadataTo(MutableDirectBuffer buffer, T toEncode, int offset) {
            metaCodec.encodeTo(buffer, toEncode, offset);
        }

        @Override
        public <T> void encodeMetadataTo(ByteBuffer buffer, T toEncode) {
            metaCodec.encodeTo(buffer, toEncode);
        }

        @Override
        public <T> T decodeData(ByteBuffer toDecode, Class<T> clazz) {
            return dataCodec.decode(toDecode, clazz);
        }

        @Override
        public <T> T decodeData(DirectBuffer toDecode, Class<T> clazz, int offset) {
            return dataCodec.decode(toDecode, offset, clazz);
        }

        @Override
        public <T> ByteBuffer encodeData(T toEncode) {
            return dataCodec.encode(toEncode);
        }

        @Override
        public <T> DirectBuffer encodeDataDirect(T toEncode) {
            return dataCodec.encodeDirect(toEncode);
        }

        @Override
        public <T> void encodeDataTo(MutableDirectBuffer buffer, T toEncode, int offset) {
            dataCodec.encodeTo(buffer, toEncode, offset);
        }

        @Override
        public <T> void encodeDataTo(ByteBuffer buffer, T toEncode) {
            dataCodec.encodeTo(buffer, toEncode);
        }
    }
}
