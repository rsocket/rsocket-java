package io.rsocket.metadata;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.util.CharsetUtil;
import io.rsocket.util.NumberUtils;
import reactor.util.annotation.Nullable;

public class CompositeMetadataFlyweight {

    static final int STREAM_METADATA_KNOWN_MASK = 0x80; // 1000 0000
    static final byte STREAM_METADATA_LENGTH_MASK = 0x7F; // 0111 1111

    /**
     * Denotes that an attempt at 0-garbage decoding failed because the input buffer
     * didn't have enough bytes to represent a complete metadata entry, only part of
     * the bytes.
     */
    public static final ByteBuf[] METADATA_MALFORMED = new ByteBuf[0];
    /**
     * Denotes that an attempt at garbage-free decoding failed because the input buffer
     * was completely empty, which generally means that no more entries are present in
     * the buffer.
     */
    public static final ByteBuf[] METADATA_BUFFERS_DONE = new ByteBuf[0];
    /**
     * Denotes that an attempt at higher level decoding of an entry components failed because
     * the input buffer was completely empty, which generally means that no more entries are
     * present in the buffer.
     */
    static final Object[] METADATA_ENTRIES_DONE = new Object[0];

    private CompositeMetadataFlyweight() {}

    //TODO document how to go from ByteBuf to id/string mime and distinguish
    public static ByteBuf[] decodeMimeAndContentBuffers(ByteBuf compositeMetadata, boolean retainSlices) {
        if (compositeMetadata.isReadable()) {
            ByteBuf mime;
            int ridx = compositeMetadata.readerIndex();
            byte mimeIdOrLength = compositeMetadata.readByte();
            if ((mimeIdOrLength & STREAM_METADATA_KNOWN_MASK) == STREAM_METADATA_KNOWN_MASK) {
                mime = retainSlices ?
                        compositeMetadata.retainedSlice(ridx, 1) :
                        compositeMetadata.slice(ridx, 1);
            }
            else {
                //M flag unset, remaining 7 bits are the length of the mime
                int mimeLength = Byte.toUnsignedInt(mimeIdOrLength) + 1;

                if (compositeMetadata.isReadable(mimeLength)) { //need to be able to read an extra mimeLength bytes
                    //here we need a way for the returned ByteBuf to differentiate between a
                    //1-byte length mime type and a 1 byte encoded mime id, preferably without
                    //re-applying the byte mask. The easiest way is to include the initial byte
                    //and have further decoding ignore the first byte. 1 byte buffer == id, 2+ byte
                    //buffer == full mime string.
                    mime = retainSlices ?
                            // we accommodate that we don't read from current readerIndex, but
                            //readerIndex - 1 ("0"), for a total slice size of mimeLength + 1
                            compositeMetadata.retainedSlice(ridx, mimeLength + 1) :
                            compositeMetadata.slice(ridx, mimeLength + 1);
                    //we thus need to skip the bytes we just sliced, but not the flag/length byte
                    //which was already skipped in initial read
                    compositeMetadata.skipBytes(mimeLength);
                }
                else {
                    return METADATA_MALFORMED;
                }
            }

            if (compositeMetadata.isReadable(3)) {
                //ensures the length medium can be read
                final int metadataLength = compositeMetadata.readUnsignedMedium();
                if (compositeMetadata.isReadable(metadataLength)) {
                    ByteBuf metadata = retainSlices ?
                            compositeMetadata.readRetainedSlice(metadataLength) :
                            compositeMetadata.readSlice(metadataLength);
                    return new ByteBuf[] { mime, metadata };
                }
                else {
                    return METADATA_MALFORMED;
                }
            }
            else {
                return METADATA_MALFORMED;
            }
        }
        return METADATA_BUFFERS_DONE;
    }

    //TODO document how to check buffer is indeed an id
    //TODO document error conditions
    public static byte decodeMimeIdFromMimeBuffer(ByteBuf mimeBuffer) {
        if (mimeBuffer.readableBytes() != 1) {
            return WellKnownMimeType.UNPARSEABLE_MIME_TYPE.getIdentifier();
        }
        return (byte) (mimeBuffer.readByte() & STREAM_METADATA_LENGTH_MASK);
    }

    //TODO document error conditions
    @Nullable
    public static CharSequence decodeMimeTypeFromMimeBuffer(ByteBuf flyweightMimeBuffer) {
        if (flyweightMimeBuffer.readableBytes() < 2) {
            return null;
        }
        //the encoded length is assumed to be kept at the start of the buffer
        //but also assumed to be irrelevant because the rest of the slice length
        //actually already matches _decoded_length
        flyweightMimeBuffer.skipBytes(1);
        int mimeStringLength = flyweightMimeBuffer.readableBytes();
        return flyweightMimeBuffer.readCharSequence(mimeStringLength, CharsetUtil.US_ASCII);
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
     */
    //see #encodeMetadataHeader(ByteBufAllocator, String, int)
    public static void encodeAndAddMetadata(CompositeByteBuf compositeMetaData, ByteBufAllocator allocator, String customMimeType, ByteBuf metadata) {
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
     */
    // see #encodeMetadataHeader(ByteBufAllocator, byte, int)
    public static void encodeAndAddMetadata(CompositeByteBuf compositeMetaData, ByteBufAllocator allocator, WellKnownMimeType knownMimeType, ByteBuf metadata) {
        compositeMetaData.addComponents(true,
                encodeMetadataHeader(allocator, knownMimeType.getIdentifier(),
                        metadata.readableBytes()), metadata);
    }

    /**
     * Encode a new sub-metadata information into a composite metadata {@link CompositeByteBuf buffer}.
     *
     * @param compositeMetaData the buffer that will hold all composite metadata information.
     * @param allocator the {@link ByteBufAllocator} to use to create intermediate buffers as needed.
     * @param unknownCompressedMimeType the id of the {@link WellKnownMimeType#UNKNOWN_RESERVED_MIME_TYPE} to encode.
     * @param metadata the metadata value to encode.
     */
    // see #encodeMetadataHeader(ByteBufAllocator, byte, int)
    static void encodeAndAddMetadata(CompositeByteBuf compositeMetaData, ByteBufAllocator allocator, byte unknownCompressedMimeType, ByteBuf metadata) {
        compositeMetaData.addComponents(true,
                encodeMetadataHeader(allocator, unknownCompressedMimeType,
                        metadata.readableBytes()), metadata);
    }

    //=== ENTRY ===

    /**
     * An entry in a Composite Metadata, which exposes the {@link #getMimeType() mime type} and {@link ByteBuf}
     * {@link #getMetadata() content} of the metadata entry.
     */
    public interface Entry {

        /**
         * Create an {@link Entry} from a {@link WellKnownMimeType}. This will be encoded in a compressed format that
         * uses the {@link WellKnownMimeType#getIdentifier() mime identifier}.
         *
         * @param mimeType the {@link WellKnownMimeType} to use for the entry
         * @param metadataContentBuffer the content {@link ByteBuf} to use for the entry
         * @return the new entry
         */
        public static Entry wellKnownMime(WellKnownMimeType mimeType, ByteBuf metadataContentBuffer) {
            return new CompressedTypeEntry(mimeType, metadataContentBuffer);
        }

        /**
         * Create an {@link Entry} from a custom mime type represented as an US-ASCII only {@link String}.
         * The whole literal mime type will thus be encoded.
         *
         * @param mimeType the custom mime type {@link String}
         * @param metadataContentBuffer the content {@link ByteBuf} to use for the entry
         * @return the new entry
         */
        public static Entry customMime(String mimeType, ByteBuf metadataContentBuffer) {
            return new CustomTypeEntry(mimeType, metadataContentBuffer);
        }

        /**
         * Create an {@link Entry} from an unrecognized yet valid "well-known" mime type, ie. a {@code byte} that would map
         * to {@link WellKnownMimeType#UNKNOWN_RESERVED_MIME_TYPE}. Prefer using {@link #wellKnownMime(WellKnownMimeType, ByteBuf)}
         * if the mime code is recognizable by this client.
         * <p>
         * This case would usually be encountered when decoding a composite metadata entry from a remote that uses a more recent
         * version of the {@link WellKnownMimeType} extension, and this method can be useful to create an unprocessed entry
         * in such a case, ensuring no information is lost when forwarding frames.
         *
         * @param mimeCode the reserved but unrecognized compressed mime type {@code byte}
         * @param metadataContentBuffer the content {@link ByteBuf} to use for the entry
         * @return the new entry
         * @see #wellKnownMime(WellKnownMimeType, ByteBuf)
         */
        public static Entry rawCompressedMime(byte mimeCode, ByteBuf metadataContentBuffer) {
            return new UnknownCompressedTypeEntry(mimeCode, metadataContentBuffer);
        }

        /**
         * Incrementally decode the next metadata entry from a {@link ByteBuf} into an {@link Entry}.
         * This is only possible on frame types used to initiate
         * interactions, if the SETUP metadata mime type was {@link WellKnownMimeType#MESSAGE_RSOCKET_COMPOSITE_METADATA}.
         * <p>
         * Each entry {@link ByteBuf} is a  {@link ByteBuf#readSlice(int) slice} of the original buffer that can also be
         * {@link ByteBuf#readRetainedSlice(int) retained} if needed.
         *
         * @param buffer the buffer to decode
         * @param retainMetadataSlices should each slide be retained when read from the original buffer?
         * @return the decoded {@link Entry}
         */
        static Entry decodeEntry(ByteBuf buffer, boolean retainMetadataSlices) {
            ByteBuf[] entry = decodeMimeAndContentBuffers(buffer, retainMetadataSlices);
            if (entry == METADATA_MALFORMED) {
                throw new IllegalArgumentException("composite metadata entry buffer is too short to contain proper entry");
            }
            if (entry == METADATA_BUFFERS_DONE) {
                return null;
            }

            ByteBuf encodedHeader = entry[0];
            ByteBuf metadataContent = entry[1];


            //the flyweight already validated the size of the buffer,
            //this is only to distinguish id vs custom type
            if (encodedHeader.readableBytes() == 1) {
                //id
                byte id = decodeMimeIdFromMimeBuffer(encodedHeader);
                WellKnownMimeType wkn = WellKnownMimeType.fromId(id);
                if (wkn == WellKnownMimeType.UNPARSEABLE_MIME_TYPE) {
                    //should not happen due to flyweight's decodeEntry own guard
                    throw new IllegalStateException("composite metadata entry parsing failed on compressed mime id " + id);
                }
                if (wkn == WellKnownMimeType.UNKNOWN_RESERVED_MIME_TYPE) {
                    return new UnknownCompressedTypeEntry(id, metadataContent);
                }
                return new CompressedTypeEntry(wkn, metadataContent);
            }
            else {
                CharSequence customMimeCharSequence = decodeMimeTypeFromMimeBuffer(encodedHeader);
                if (customMimeCharSequence == null) {
                    //should not happen due to flyweight's decodeEntry own guard
                    throw new IllegalArgumentException("composite metadata entry parsing failed on custom type");
                }
                return new CustomTypeEntry(customMimeCharSequence.toString(), metadataContent);
            }
        }

        /**
         * A <i>passthrough</i> entry is one for which the {@link #getMimeType()} could not be decoded.
         * This is usually because it was compressed on the wire, but using an id that is still just "reserved for
         * future use" in this implementation.
         * <p>
         * Still, another actor on the network might be able to interpret such an entry, which should thus be
         * re-encoded as it was when forwarding the frame.
         * <p>
         * The {@link #getMetadata()} exposes the raw content buffer of the entry (as any other entry).
         *
         * @return true if this entry should be ignored but passed through as is during re-encoding
         */
        default boolean isPassthrough() {
            return false;
        }

        /**
         * @return the mime type for this entry
         */
        @Nullable
        String getMimeType();

        /**
         * @return the metadata content of this entry
         */
        ByteBuf getMetadata();

        /**
         * Encode this {@link Entry} into a {@link CompositeByteBuf} representing a composite metadata.
         * This buffer may already hold components for previous {@link Entry entries}.
         *
         * @param compositeByteBuf the {@link CompositeByteBuf} to hold the components of the whole composite metadata
         * @param byteBufAllocator the {@link ByteBufAllocator} to use to allocate new buffers as needed
         */
        default void encodeInto(CompositeByteBuf compositeByteBuf, ByteBufAllocator byteBufAllocator) {
            if (this instanceof CompressedTypeEntry) {
                CompressedTypeEntry cte = (CompressedTypeEntry) this;
                encodeAndAddMetadata(compositeByteBuf, byteBufAllocator,
                        cte.mimeType, cte.content);
            }
            else if (this instanceof UnknownCompressedTypeEntry) {
                UnknownCompressedTypeEntry ucte = (UnknownCompressedTypeEntry) this;
                encodeAndAddMetadata(compositeByteBuf, byteBufAllocator,
                        (byte) ucte.identifier, ucte.content);
            }
            else {
                encodeAndAddMetadata(compositeByteBuf, byteBufAllocator,
                        getMimeType(), getMetadata());
            }
        }
    }

    static final class CustomTypeEntry implements Entry {

        private final String mimeType;
        private final ByteBuf content;

        CustomTypeEntry(String mimeType, ByteBuf content) {
            this.mimeType = mimeType;
            this.content = content;
        }

        @Override
        public String getMimeType() {
            return this.mimeType;
        }

        @Override
        public ByteBuf getMetadata() {
            return this.content;
        }
    }

    static final class CompressedTypeEntry implements Entry {

        private final WellKnownMimeType mimeType;
        private final ByteBuf content;

        CompressedTypeEntry(WellKnownMimeType mimeType, ByteBuf content) {
            this.mimeType = mimeType;
            this.content = content;
        }

        @Override
        public String getMimeType() {
            return this.mimeType.getMime();
        }

        @Override
        public ByteBuf getMetadata() {
            return this.content;
        }
    }

    /**
     * A {@link Entry#isPassthrough() pass-through} {@link Entry} that encapsulates a
     * compressed-mime metadata that couldn't be recognized.
     */
    static final class UnknownCompressedTypeEntry implements Entry {

        private final byte identifier;
        private final ByteBuf content;

        UnknownCompressedTypeEntry(byte identifier, ByteBuf content) {
            this.identifier = identifier;
            this.content = content;
        }

        @Override
        public boolean isPassthrough() {
            return true;
        }

        @Override
        public String getMimeType() {
            return WellKnownMimeType.UNKNOWN_RESERVED_MIME_TYPE.getMime();
        }

        /**
         * Return the compressed identifier that was used in the original decoded metadata, but couldn't be
         * decoded because this implementation only knows this as "reserved for future use". The original
         */
        public byte getUnknownReservedId() {
            return this.identifier;
        }

        @Override
        public ByteBuf getMetadata() {
            return this.content;
        }
    }
}
