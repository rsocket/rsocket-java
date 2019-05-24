package io.rsocket.metadata;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.util.CharsetUtil;
import io.rsocket.util.NumberUtils;

class CompositeMetadataFlyweight {


    static final int STREAM_METADATA_KNOWN_MASK = 0x80; // 1000 0000
    static final byte STREAM_METADATA_LENGTH_MASK = 0x7F; // 0111 1111

    private CompositeMetadataFlyweight() {}

    /**
     * Decode the next mime type information from a composite metadata buffer which {@link ByteBuf#readerIndex()} is
     * at the start of the next metadata section.
     * <p>
     * By order of preference the mime type is returned as a {@link WellKnownMimeType} (if encoded as such and recognizable),
     * a {@code byte} (if encoded as a {@link WellKnownMimeType} id that is valid BUT unrecognized) or a {@link String}
     * containing only US_ASCII characters. The index is moved past the mime section, to the starting byte of the
     * sub-metadata's length.
     *
     * @param buffer the metadata or composite metadata to read mime information from.
     * @return the next metadata mime type as {@link String}, a {@link WellKnownMimeType} or a {@code byte}. the buffer {@link ByteBuf#readerIndex()} is moved.
     */
    static Object decode3WaysMimeFromMetadataHeader(ByteBuf buffer) {
        byte source = buffer.readByte();
        if ((source & STREAM_METADATA_KNOWN_MASK) == STREAM_METADATA_KNOWN_MASK) {
            //M flag set
            int id = source & STREAM_METADATA_LENGTH_MASK;
            WellKnownMimeType mime = WellKnownMimeType.fromId(id);
            if (mime == WellKnownMimeType.UNPARSEABLE_MIME_TYPE) {
                //should not be possible with the mask
                throw new IllegalStateException("Mime compression flag detected, but invalid mime id " + id);
            }
            if (mime == WellKnownMimeType.UNKNOWN_RESERVED_MIME_TYPE) {
                return (byte) id;
            }
            return mime;
        }
        //M flag unset, remaining 7 bits are the length of the mime
        int mimeLength = Byte.toUnsignedInt(source) + 1;
        CharSequence mime = buffer.readCharSequence(mimeLength, CharsetUtil.US_ASCII);
        return mime.toString();
    }

    /**
     * Decode the current metadata length information from a composite metadata buffer which {@link ByteBuf#readerIndex()}
     * is just past the current metadata section's mime information.
     * <p>
     * The index is moved past the metadata length section, to the starting byte of the current metadata's value.
     *
     * @param buffer the metadata or composite metadata to read length information from.
     * @return the next metadata length. the buffer {@link ByteBuf#readerIndex()} is moved.
     */
    static int decodeMetadataLengthFromMetadataHeader(ByteBuf buffer) {
        if (buffer.readableBytes() < 3) {
            throw new IllegalStateException("the given buffer should contain at least 3 readable bytes after decoding mime type");
        }
        return buffer.readUnsignedMedium();
    }

    /**
     * Decode the next composite metadata piece from a composite metadata buffer into an {@link Object} array which
     * holds two elements: the mime type (as either a {@code byte}, a {@link String} or a {@link WellKnownMimeType}) and
     * the {@link ByteBuf} metadata value.
     * The array is empty if the composite metadata buffer has been entirely decoded, but generally this method shouldn't
     * be called if the buffer's {@link ByteBuf#isReadable()} method returns {@code false}.
     *
     * @param compositeMetadata the {@link ByteBuf} that contains information for one or more metadata mime-value pairs,
     *                          with its reader index set at the start of next metadata piece (or end of the buffer if
     *                          fully decoded)
     * @param retainMetadataSlices should metadata value {@link ByteBuf} be {@link ByteBuf#retain() retained} when decoded?
     * @return the decoded piece of composite metadata, or an empty Object array if no more metadata is in the composite
     */
    static Object[] decodeNext(ByteBuf compositeMetadata, boolean retainMetadataSlices) {
        if (compositeMetadata.isReadable()) {
            Object mime = decode3WaysMimeFromMetadataHeader(compositeMetadata);
            int length = decodeMetadataLengthFromMetadataHeader(compositeMetadata);
            ByteBuf metadata = retainMetadataSlices ? compositeMetadata.readRetainedSlice(length) : compositeMetadata.readSlice(length);

            return new Object[] {mime, metadata};
        }
        return new Object[0];
    }

    /**
     * Encode a {@link WellKnownMimeType well known mime type} and a metadata value length into a newly allocated
     * {@link ByteBuf}.
     * <p>
     * This compact representation encodes the mime type via its ID on a single byte, and the unsigned value length on
     * 3 additional bytes.
     *
     * @param allocator the {@link ByteBufAllocator} to use to create the buffer.
     * @param mimeType a byte identifier of a {@link WellKnownMimeType} to encode.
     * @param metadataLength the metadata length to append to the buffer as an unsigned 24 bits integer.
     * @return the encoded mime and metadata length information
     */
    static ByteBuf encodeMetadataHeader(ByteBufAllocator allocator, byte mimeType, int metadataLength) {
        byte id = (byte) (mimeType & 0xFF);

        ByteBuf buffer = allocator.buffer(4, 4)
                .writeByte(mimeType | STREAM_METADATA_KNOWN_MASK);

        NumberUtils.encodeUnsignedMedium(buffer, metadataLength);

        return buffer;
    }

    /**
     * Encode a custom mime type and a metadata value length into a newly allocated {@link ByteBuf}.
     * <p>
     * This larger representation encodes the mime type representation's length on a single byte, then the representation
     * itself, then the unsigned metadata value length on 3 additional bytes.
     *
     * @param allocator the {@link ByteBufAllocator} to use to create the buffer.
     * @param customMime a custom mime type to encode.
     * @param metadataLength the metadata length to append to the buffer as an unsigned 24 bits integer.
     * @return the encoded mime and metadata length information
     */
    static ByteBuf encodeMetadataHeader(ByteBufAllocator allocator, String customMime, int metadataLength) {
        ByteBuf mimeBuffer = allocator.buffer(customMime.length());
        mimeBuffer.writeCharSequence(customMime, CharsetUtil.UTF_8);
        if (!ByteBufUtil.isText(mimeBuffer, CharsetUtil.US_ASCII)) {
            throw new IllegalArgumentException("custom mime type must be US_ASCII characters only");
        }
        int ml = mimeBuffer.readableBytes();
        if (ml < 1 || ml > 128) {
            throw new IllegalArgumentException("custom mime type must have a strictly positive length that fits on 7 unsigned bits, ie 1-128");
        }
        ml--;

        ByteBuf mimeLength = allocator.buffer(1,1);
        mimeLength.writeByte((byte) ml);

        ByteBuf metadataLengthBuffer = allocator.buffer(3, 3);
        NumberUtils.encodeUnsignedMedium(metadataLengthBuffer, metadataLength);

        return allocator.compositeBuffer()
                .addComponents(true, mimeLength, mimeBuffer, metadataLengthBuffer);
    }

    /**
     * Encode a new sub-metadata information into a composite metadata {@link CompositeByteBuf buffer}.
     *
     * @param compositeMetaData the buffer that will hold all composite metadata information.
     * @param allocator the {@link ByteBufAllocator} to use to create intermediate buffers as needed.
     * @param customMimeType the custom mime type to encode.
     * @param metadata the metadata value to encode.
     * @see #encodeMetadataHeader(ByteBufAllocator, String, int)
     */
    static void addMetadata(CompositeByteBuf compositeMetaData, ByteBufAllocator allocator, String customMimeType, ByteBuf metadata) {
        compositeMetaData.addComponents(true,
                encodeMetadataHeader(allocator, customMimeType, metadata.readableBytes()),
                metadata);
    }

    /**
     * Encode a new sub-metadata information into a composite metadata {@link CompositeByteBuf buffer}.
     *
     * @param compositeMetaData the buffer that will hold all composite metadata information.
     * @param allocator the {@link ByteBufAllocator} to use to create intermediate buffers as needed.
     * @param knownMimeType the {@link WellKnownMimeType} to encode.
     * @param metadata the metadata value to encode.
     * @see #encodeMetadataHeader(ByteBufAllocator, byte, int)
     */
    static void addMetadata(CompositeByteBuf compositeMetaData, ByteBufAllocator allocator, WellKnownMimeType knownMimeType, ByteBuf metadata) {
        compositeMetaData.addComponents(true,
                encodeMetadataHeader(allocator, knownMimeType.getIdentifier(), metadata.readableBytes()),
                metadata);
    }

    /**
     * Encode a new sub-metadata information into a composite metadata {@link CompositeByteBuf buffer}.
     *
     * @param compositeMetaData the buffer that will hold all composite metadata information.
     * @param allocator the {@link ByteBufAllocator} to use to create intermediate buffers as needed.
     * @param unknownCompressedMimeType the id of the {@link WellKnownMimeType#UNKNOWN_RESERVED_MIME_TYPE} to encode.
     * @param metadata the metadata value to encode.
     * @see #encodeMetadataHeader(ByteBufAllocator, byte, int)
     */
    static void addMetadata(CompositeByteBuf compositeMetaData, ByteBufAllocator allocator, byte unknownCompressedMimeType, ByteBuf metadata) {
        compositeMetaData.addComponents(true,
                encodeMetadataHeader(allocator, unknownCompressedMimeType, metadata.readableBytes()),
                metadata);
    }

}
