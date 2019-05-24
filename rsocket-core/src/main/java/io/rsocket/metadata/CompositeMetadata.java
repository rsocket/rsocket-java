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
public interface CompositeMetadata {

    /**
     * An entry in a {@link CompositeMetadata}, which exposes the {@link #getMimeType() mime type} and {@link ByteBuf}
     * {@link #getMetadata() content} of the metadata entry.
     */
    interface Entry {

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
    static CompositeMetadata decode(ByteBuf buffer) {
        List<Entry> entries = new ArrayList<>();
        while (buffer.isReadable()) {
            entries.add(decodeIncrementally(buffer, true));
        }
        return new DefaultCompositeMetadata(entries);
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
    static Entry decodeIncrementally(ByteBuf buffer, boolean retainMetadataSlices) {
        Object[] entry = CompositeMetadataFlyweight.decodeNext(buffer, retainMetadataSlices);
        Object mime = entry[0];
        ByteBuf buf = (ByteBuf) entry[1];
        if (mime instanceof WellKnownMimeType) {
            return new CompressedTypeEntry((WellKnownMimeType) mime, buf);
        }
        if (mime instanceof Byte) {
            return new UnknownCompressedTypeEntry((Byte) mime, buf);
        }
        return new CustomTypeEntry((String) mime, buf);
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
    static CompositeByteBuf encode(ByteBufAllocator allocator, CompositeMetadata metadata) {
        CompositeByteBuf compositeMetadataBuffer = allocator.compositeBuffer();
        for (Entry entry : metadata.getAll()) {
            encodeIncrementally(allocator, compositeMetadataBuffer, entry);
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
    static void encodeIncrementally(ByteBufAllocator allocator, CompositeByteBuf compositeMetadataBuffer, Entry metadataEntry) {
        if (metadataEntry instanceof UnknownCompressedTypeEntry) {
            byte id = ((UnknownCompressedTypeEntry) metadataEntry).getUnknownReservedId();
            CompositeMetadataFlyweight.addMetadata(compositeMetadataBuffer, allocator, id, metadataEntry.getMetadata());
        }
        else if (metadataEntry instanceof CompressedTypeEntry) {
            WellKnownMimeType mimeType = ((CompressedTypeEntry) metadataEntry).mimeType;
            CompositeMetadataFlyweight.addMetadata(compositeMetadataBuffer, allocator, mimeType, metadataEntry.getMetadata());
        }
        else {
            CompositeMetadataFlyweight.addMetadata(compositeMetadataBuffer, allocator, metadataEntry.getMimeType(), metadataEntry.getMetadata());
        }
    }

    /**
     * Get the first {@link CompositeMetadata.Entry} that matches the given mime type, or null if no such
     * entry exist.
     *
     * @param mimeTypeKey the mime type to look up
     * @return the metadata entry
     */
    Entry get(String mimeTypeKey);

    /**
     * Get the {@link CompositeMetadata.Entry} at the given 0-based index. This is equivalent to
     * {@link #getAll()}{@link List#get(int) .get(index)}.
     *
     * @param index the index to look up
     * @return the metadata entry
     * @throws IndexOutOfBoundsException if the index is negative or greater than or equal to {@link #size()}
     */
    Entry get(int index);

    /**
     * Get all entries that match the given mime type. An empty list is returned if no such entry exists.
     * Entries are presented in an unmodifiable {@link List} in the order they appeared in on the wire.
     *
     * @param mimeTypeKey the mime type to look up
     * @return an unmodifiable {@link List} of matching entries in the composite
     */
    List<Entry> getAll(String mimeTypeKey);

    /**
     * Get all entries in the composite, to the exclusion of entries which mime type cannot be parsed,
     * but were marked as {@link Entry#isPassthrough()} during decoding.
     *
     * @return an unmodifiable {@link List} of all the (parseable) entries in the composite
     */
    List<Entry> getAllParseable();

    /**
     * Get all entries in the composite. Entries are presented in an unmodifiable {@link List} in the
     * order they appeared in on the wire.
     *
     * @return an unmodifiable {@link List} of all the entries in the composite
     */
    List<Entry> getAll();

    /**
     * Get the number of entries in this composite.
     *
     * @return the size of the metadata composite
     */
    int size();

    final class DefaultCompositeMetadata implements CompositeMetadata {

        List<Entry> entries;

        DefaultCompositeMetadata(List<Entry> entries) {
            this.entries = Collections.unmodifiableList(entries);
        }

        @Override
        public Entry get(String mimeTypeKey) {
            for (Entry entry : entries) {
                if (mimeTypeKey.equals(entry.getMimeType())) {
                    return entry;
                }
            }
            return null;
        }

        @Override
        public Entry get(int index) {
            return entries.get(index);
        }

        @Override
        public List<Entry> getAll(String mimeTypeKey) {
            List<Entry> forMimeType = new ArrayList<>();
            for (Entry entry : entries) {
                if (mimeTypeKey.equals(entry.getMimeType())) {
                    forMimeType.add(entry);
                }
            }
            return Collections.unmodifiableList(forMimeType);
        }

        @Override
        public List<Entry> getAllParseable() {
            List<Entry> notPassthrough = new ArrayList<>();
            for (Entry entry : entries) {
                if (!entry.isPassthrough()) {
                    notPassthrough.add(entry);
                }
            }
            return Collections.unmodifiableList(notPassthrough);
        }

        @Override
        public List<Entry> getAll() {
            return this.entries; //initially wrapped in Collections.unmodifiableList, so safe to return
        }

        @Override
        public int size() {
            return entries.size();
        }
    }

    final class CustomTypeEntry implements Entry {

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

    final class CompressedTypeEntry implements Entry {

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

    final class UnknownCompressedTypeEntry implements Entry {

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
