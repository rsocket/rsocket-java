package io.rsocket.metadata;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static io.netty.buffer.Unpooled.buffer;

/**
 * Tagging metadata from https://github.com/rsocket/rsocket/blob/master/Extensions/Routing.md
 *
 * @author linux_china
 */
public class TaggingMetadata implements Iterable<String>, CompositeMetadata.Entry {
    /**
     * Tag max length in bytes
     */
    private static int TAG_LENGTH_MAX = 0xFF;
    private String type;
    private ByteBuf content;

    public TaggingMetadata(String type, ByteBuf content) {
        this.type = type;
        this.content = content;
    }

    public TaggingMetadata(String type, Collection<String> tags) {
        this.type = type;
        CompositeByteBuf compositeByteBuf = Unpooled.compositeBuffer();
        tags.stream()
                .map(tag -> tag.getBytes(StandardCharsets.UTF_8))
                .filter(bytes -> bytes.length > 0 && bytes.length < TAG_LENGTH_MAX)
                .forEach(bytes -> {
                    int capacity = bytes.length + 1;
                    ByteBuf tagByteBuf = buffer(capacity, capacity);
                    tagByteBuf.writeByte(bytes.length);
                    tagByteBuf.writeBytes(bytes);
                    compositeByteBuf.addComponent(true, tagByteBuf);
                });
        this.content = compositeByteBuf;
    }

    public Stream<String> stream() {
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                        iterator(), Spliterator.DISTINCT | Spliterator.NONNULL | Spliterator.ORDERED),
                false);
    }

    @Override
    public Iterator<String> iterator() {
        return new Iterator<String>() {
            @Override
            public boolean hasNext() {
                return content.readerIndex() < content.capacity();
            }

            @Override
            public String next() {
                int tagLength = TAG_LENGTH_MAX & content.readByte();
                if (tagLength > 0) {
                    byte[] tagBytes = new byte[tagLength];
                    content.readBytes(tagBytes);
                    return new String(tagBytes, StandardCharsets.UTF_8);
                } else {
                    return "";
                }
            }
        };
    }

    @Override
    public ByteBuf getContent() {
        return this.content;
    }

    @Override
    public String getMimeType() {
        return this.type;
    }
}
