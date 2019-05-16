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
            Object[] entry = CompositeMetadataFlyweight.decodeNext(buffer, true);
            String mime = (String) entry[0];
            ByteBuf buf = (ByteBuf) entry[1];
            entries.add(new DefaultEntry(mime, buf));
        }
        return new DefaultCompositeMetadata(entries);
    }

    /**
     * Encode the next part of a composite metadata into a pre-existing {@link CompositeByteBuf} (which can be empty if
     * the first part is being encoded).
     * <p>
     * This method moves the composite buffer's {@link ByteBuf#writerIndex()}, and internally allocates one buffer for
     * the metadata header using the provided {@link ByteBufAllocator}. The mime type is compressed using the
     * {@link WellKnownMimeType}'s identifier.
     *
     * @param allocator the {@link ByteBufAllocator} to use to encode the metadata headers (mime type id, metadata length)
     * @param compositeMetadataBuffer the composite buffer to which this metadata piece is added
     * @param mimeType the {@link WellKnownMimeType} to use
     * @param contentBuffer the metadata content
     */
    static void encode(ByteBufAllocator allocator, CompositeByteBuf compositeMetadataBuffer, WellKnownMimeType mimeType, ByteBuf contentBuffer) {
        CompositeMetadataFlyweight.addMetadata(compositeMetadataBuffer, allocator, mimeType, contentBuffer);
    }

    /**
     * Encode the next part of a composite metadata into a pre-existing {@link CompositeByteBuf} (which can be empty if
     * the first part is being encoded).
     * <p>
     * This method moves the composite buffer's {@link ByteBuf#writerIndex()}, and internally allocates one buffer for
     * the metadata header using the provided {@link ByteBufAllocator}. The mime type is NOT compressed, even if it
     * matches a {@link WellKnownMimeType}, see {@link #encode(ByteBufAllocator, CompositeByteBuf, String, ByteBuf)}.
     *
     * @param allocator the {@link ByteBufAllocator} to use to encode the metadata headers (mime type length, mime type, metadata length)
     * @param compositeMetadataBuffer the composite buffer to which this metadata piece is added
     * @param customMimeType the custom mime type to use (always encoded as length + string)
     * @param contentBuffer the metadata content
     * @see #encode(ByteBufAllocator, CompositeByteBuf, String, ByteBuf)
     */
    static void encodeWithoutMimeCompression(ByteBufAllocator allocator, CompositeByteBuf compositeMetadataBuffer, String customMimeType, ByteBuf contentBuffer) {
        CompositeMetadataFlyweight.addMetadata(compositeMetadataBuffer, allocator, customMimeType, contentBuffer);
    }

    /**
     * Encode the next part of a composite metadata into a pre-existing {@link CompositeByteBuf} (which can be empty if
     * the first part is being encoded), with attempted mime type compression.
     * <p>
     * This method moves the composite buffer's {@link ByteBuf#writerIndex()}, and internally allocates one buffer for
     * the metadata header using the provided {@link ByteBufAllocator}. Compression of the mime type is attempted, by
     * first trying to find a matching {@link WellKnownMimeType} and using its identifier instead of a length + type
     * encoding. Use {@link #encode(ByteBufAllocator, CompositeByteBuf, WellKnownMimeType, ByteBuf)} directly if you
     * already know the {@link WellKnownMimeType} value to use.
     *
     * @param allocator the {@link ByteBufAllocator} to use to encode the metadata headers: (mime type length+mime type)
     *                  OR mime type id, metadata length
     * @param compositeMetadataBuffer the composite buffer to which this metadata piece is added
     * @param customOrKnownMimeType the custom mime type to use (only encoded as length+string if no {@link WellKnownMimeType} can be found)
     * @param contentBuffer the metadata content
     */
    static void encode(ByteBufAllocator allocator, CompositeByteBuf compositeMetadataBuffer, String customOrKnownMimeType, ByteBuf contentBuffer) {
        WellKnownMimeType knownType;
        try {
            knownType = WellKnownMimeType.fromMimeType(customOrKnownMimeType);
        }
        catch (IllegalArgumentException iae) {
            CompositeMetadataFlyweight.addMetadata(compositeMetadataBuffer, allocator, customOrKnownMimeType, contentBuffer);
            return;
        }
        CompositeMetadataFlyweight.addMetadata(compositeMetadataBuffer, allocator, knownType, contentBuffer);
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

        private DefaultCompositeMetadata(List<Entry> entries) {
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
            return forMimeType;
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

    final class DefaultEntry implements Entry {

        private final String mimeType;
        private final ByteBuf content;

        DefaultEntry(String mimeType, ByteBuf content) {
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

}
