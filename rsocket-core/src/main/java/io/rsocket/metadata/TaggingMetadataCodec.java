package io.rsocket.metadata;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import java.nio.charset.StandardCharsets;
import java.util.Collection;

/**
 * A flyweight class that can be used to encode/decode tagging metadata information to/from {@link
 * ByteBuf}. This is intended for low-level efficient manipulation of such buffers. See {@link
 * TaggingMetadata} for an Iterator-like approach to decoding entries.
 *
 * @author linux_china
 */
public class TaggingMetadataCodec {
  /** Tag max length in bytes */
  private static int TAG_LENGTH_MAX = 0xFF;

  /**
   * create routing metadata
   *
   * @param allocator the {@link ByteBufAllocator} to use to create intermediate buffers as needed.
   * @param tags tag values
   * @return routing metadata
   */
  public static RoutingMetadata createRoutingMetadata(
      ByteBufAllocator allocator, Collection<String> tags) {
    return new RoutingMetadata(createTaggingContent(allocator, tags));
  }

  /**
   * create tagging metadata from composite metadata entry
   *
   * @param entry composite metadata entry
   * @return tagging metadata
   */
  public static TaggingMetadata createTaggingMetadata(CompositeMetadata.Entry entry) {
    return new TaggingMetadata(entry.getMimeType(), entry.getContent());
  }

  /**
   * create tagging metadata
   *
   * @param allocator the {@link ByteBufAllocator} to use to create intermediate buffers as needed.
   * @param knownMimeType the {@link WellKnownMimeType} to encode.
   * @param tags tag values
   * @return Tagging Metadata
   */
  public static TaggingMetadata createTaggingMetadata(
      ByteBufAllocator allocator, String knownMimeType, Collection<String> tags) {
    return new TaggingMetadata(knownMimeType, createTaggingContent(allocator, tags));
  }

  /**
   * create tagging content
   *
   * @param allocator the {@link ByteBufAllocator} to use to create intermediate buffers as needed.
   * @param tags tag values
   * @return tagging content
   */
  public static ByteBuf createTaggingContent(ByteBufAllocator allocator, Collection<String> tags) {
    CompositeByteBuf taggingContent = allocator.compositeBuffer();
    for (String key : tags) {
      int length = ByteBufUtil.utf8Bytes(key);
      if (length == 0 || length > TAG_LENGTH_MAX) {
        continue;
      }
      ByteBuf byteBuf = allocator.buffer().writeByte(length);
      byteBuf.writeCharSequence(key, StandardCharsets.UTF_8);
      taggingContent.addComponent(true, byteBuf);
    }
    return taggingContent;
  }
}
