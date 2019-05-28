package io.rsocket.metadata;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A composite metadata, made of one or more {@link CompositeMetadata.Entry}. Each entry gives access to the
 * mime type it uses for metadata encoding, but it is permitted that several entries for the same mime type exist.
 * Getting an entry by mime type only returns the first matching entry in such a case, so the {@link #getAll(String)}
 * alternative is recommended. Both this method and the {@link #getAll()} method return a read-only view of the
 * composite.
 */
public final class CompositeMetadata {

    /**
     * An entry in a {@link CompositeMetadata}, which exposes the {@link #getMimeType() mime type} and {@link ByteBuf}
     * {@link #getMetadata() content} of the metadata entry.
     */
    public interface Entry {

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
        String getMimeType();

        /**
         * @return the metadata content of this entry
         */
        ByteBuf getMetadata();
    }


    /**
     * Decode a {@link ByteBuf} into a {@link CompositeMetadata}. This is only possible on frame types used to initiate
     * interactions, if the SETUP metadata mime type was {@link WellKnownMimeType#MESSAGE_RSOCKET_COMPOSITE_METADATA}.
     * <p>
     * Each entry {@link ByteBuf} is a {@link ByteBuf#readRetainedSlice(int) retained slice} of the original buffer.
     *
     * @param buffer the buffer to decode
     * @return the decoded {@link CompositeMetadata}
     */
    public static CompositeMetadata decodeComposite(ByteBuf buffer) {
        List<Entry> entries = new ArrayList<>();
        while (buffer.isReadable()) {
            entries.add(decodeEntry(buffer, true));
        }
        return new CompositeMetadata(entries);
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
    public static Entry decodeEntry(ByteBuf buffer, boolean retainMetadataSlices) {
        ByteBuf[] entry = CompositeMetadataFlyweight.decodeMimeAndContentBuffers(buffer, retainMetadataSlices);
        if (entry == CompositeMetadataFlyweight.METADATA_MALFORMED) {
            throw new IllegalArgumentException("composite metadata entry buffer is too short to contain proper entry");
        }
        if (entry == CompositeMetadataFlyweight.METADATA_BUFFERS_DONE) {
            return null;
        }

        ByteBuf encodedHeader = entry[0];
        ByteBuf metadataContent = entry[1];


        //the flyweight already validated the size of the buffer,
        //this is only to distinguish id vs custom type
        if (encodedHeader.readableBytes() == 1) {
            //id
            byte id = CompositeMetadataFlyweight.decodeMimeIdFromMimeBuffer(encodedHeader);
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
            CharSequence customMimeCharSequence = CompositeMetadataFlyweight.decodeMimeTypeFromMimeBuffer(encodedHeader);
            if (customMimeCharSequence == null) {
                //should not happen due to flyweight's decodeEntry own guard
                throw new IllegalArgumentException("composite metadata entry parsing failed on custom type");
            }
            return new CustomTypeEntry(customMimeCharSequence.toString(), metadataContent);
        }
    }

    /**
     * Encode a {@link CompositeMetadata} into a new {@link CompositeByteBuf}.
     * <p>
     * This method moves the buffer's {@link ByteBuf#writerIndex()}.
     * It uses the existing content {@link ByteBuf} of each {@link Entry}, but allocates a new buffer for each metadata
     * header using the provided {@link ByteBufAllocator}.
     *
     * @param allocator the {@link ByteBufAllocator} to use when a new buffer is needed
     * @param metadata the {@link CompositeMetadata} to encode
     * @return a {@link CompositeByteBuf} that represents the {@link CompositeMetadata}
     */
    public static CompositeByteBuf encodeComposite(ByteBufAllocator allocator, CompositeMetadata metadata) {
        CompositeByteBuf compositeMetadataBuffer = allocator.compositeBuffer();
        for (Entry entry : metadata.getAll()) {
            encodeEntry(allocator, compositeMetadataBuffer, entry);
        }
        return compositeMetadataBuffer;
    }

    /**
     * Incrementally encode a {@link CompositeMetadata} by encoding a provided {@link Entry} into a pre-existing
     * {@link CompositeByteBuf} (which can be empty if it is the first entry that is being encoded).
     * <p>
     * This method moves the composite buffer's {@link ByteBuf#writerIndex()}, and internally allocates one buffer for
     * the metadata header using the provided {@link ByteBufAllocator}.
     * <p>
     * If the mime type is either a {@link WellKnownMimeType} or a {@link WellKnownMimeType#UNKNOWN_RESERVED_MIME_TYPE},
     * it is compressed using the identifier. Otherwise, the {@link String} length + mime type are encoded.
     *
     * @param allocator the {@link ByteBufAllocator} to use to encode the metadata headers (mime type id/length+string, metadata length)
     * @param compositeMetadataBuffer the composite buffer to which this metadata piece is added
     * @param metadataEntry the {@link Entry} to encode
     */
    public static void encodeEntry(ByteBufAllocator allocator, CompositeByteBuf compositeMetadataBuffer, Entry metadataEntry) {
        if (metadataEntry instanceof UnknownCompressedTypeEntry) {
            byte id = ((UnknownCompressedTypeEntry) metadataEntry).getUnknownReservedId();
            CompositeMetadataFlyweight.encodeAndAddMetadata(compositeMetadataBuffer, allocator, id, metadataEntry.getMetadata());
        }
        else if (metadataEntry instanceof CompressedTypeEntry) {
            WellKnownMimeType mimeType = ((CompressedTypeEntry) metadataEntry).mimeType;
            CompositeMetadataFlyweight.encodeAndAddMetadata(compositeMetadataBuffer, allocator, mimeType, metadataEntry.getMetadata());
        }
        else {
            CompositeMetadataFlyweight.encodeAndAddMetadata(compositeMetadataBuffer, allocator, metadataEntry.getMimeType(), metadataEntry.getMetadata());
        }
    }

    List<Entry> entries;

    CompositeMetadata(List<Entry> entries) {
        this.entries = Collections.unmodifiableList(entries);
    }

    /**
     * Get the first {@link CompositeMetadata.Entry} that matches the given mime type, or null if no such
     * entry exist.
     *
     * @param mimeTypeKey the mime type to look up
     * @return the metadata entry
     */
    public Entry get(String mimeTypeKey) {
        for (Entry entry : entries) {
            if (mimeTypeKey.equals(entry.getMimeType())) {
                return entry;
            }
        }
        return null;
    }

    /**
     * Get the {@link CompositeMetadata.Entry} at the given 0-based index. This is equivalent to
     * {@link #getAll()}{@link List#get(int) .get(index)}.
     *
     * @param index the index to look up
     * @return the metadata entry
     * @throws IndexOutOfBoundsException if the index is negative or greater than or equal to {@link #size()}
     */
    public Entry get(int index) {
        return entries.get(index);
    }

    /**
     * Get all entries that match the given mime type. An empty list is returned if no such entry exists.
     * Entries are presented in an unmodifiable {@link List} in the order they appeared in on the wire.
     *
     * @param mimeTypeKey the mime type to look up
     * @return an unmodifiable {@link List} of matching entries in the composite
     */
    public List<Entry> getAll(String mimeTypeKey) {
        List<Entry> forMimeType = new ArrayList<>();
        for (Entry entry : entries) {
            if (mimeTypeKey.equals(entry.getMimeType())) {
                forMimeType.add(entry);
            }
        }
        return Collections.unmodifiableList(forMimeType);
    }

    /**
     * Get all entries in the composite, to the exclusion of entries which mime type cannot be parsed,
     * but were marked as {@link Entry#isPassthrough()} during decoding.
     *
     * @return an unmodifiable {@link List} of all the (parseable) entries in the composite
     */
    public List<Entry> getAllParseable() {
        List<Entry> notPassthrough = new ArrayList<>();
        for (Entry entry : entries) {
            if (!entry.isPassthrough()) {
                notPassthrough.add(entry);
            }
        }
        return Collections.unmodifiableList(notPassthrough);
    }

    /**
     * Get all entries in the composite. Entries are presented in an unmodifiable {@link List} in the
     * order they appeared in on the wire.
     *
     * @return an unmodifiable {@link List} of all the entries in the composite
     */
    public List<Entry> getAll() {
        return this.entries; //initially wrapped in Collections.unmodifiableList, so safe to return
    }

    /**
     * Get the number of entries in this composite.
     *
     * @return the size of the metadata composite
     */
    public int size() {
        return entries.size();
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
    public static final class UnknownCompressedTypeEntry implements Entry {

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
