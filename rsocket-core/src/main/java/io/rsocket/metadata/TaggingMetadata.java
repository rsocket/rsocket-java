package io.rsocket.metadata;

import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Tagging metadata from https://github.com/rsocket/rsocket/blob/master/Extensions/Routing.md
 *
 * @author linux_china
 */
public class TaggingMetadata implements Iterable<String>, CompositeMetadata.Entry {
  /** Tag max length in bytes */
  private static int TAG_LENGTH_MAX = 0xFF;

  private String mimeType;
  private ByteBuf content;

  public TaggingMetadata(String mimeType, ByteBuf content) {
    this.mimeType = mimeType;
    this.content = content;
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
          return content.readSlice(tagLength).toString(StandardCharsets.UTF_8);
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
    return this.mimeType;
  }
}
