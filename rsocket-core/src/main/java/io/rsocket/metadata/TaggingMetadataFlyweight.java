package io.rsocket.metadata;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.util.Collection;

/**
 * A flyweight class that can be used to encode/decode tagging metadata information to/from {@link
 * ByteBuf}. This is intended for low-level efficient manipulation of such buffers. See {@link
 * TaggingMetadata} for an Iterator-like approach to decoding entries.
 *
 * @deprecated in favor of {@link TaggingMetadataCodec}
 * @author linux_china
 */
@Deprecated
public class TaggingMetadataFlyweight {
  /**
   * create routing metadata
   *
   * @param allocator the {@link ByteBufAllocator} to use to create intermediate buffers as needed.
   * @param tags tag values
   * @return routing metadata
   */
  public static RoutingMetadata createRoutingMetadata(
      ByteBufAllocator allocator, Collection<String> tags) {
    return TaggingMetadataCodec.createRoutingMetadata(allocator, tags);
  }

  /**
   * create tagging metadata from composite metadata entry
   *
   * @param entry composite metadata entry
   * @return tagging metadata
   */
  public static TaggingMetadata createTaggingMetadata(CompositeMetadata.Entry entry) {
    return TaggingMetadataCodec.createTaggingMetadata(entry);
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
    return TaggingMetadataCodec.createTaggingMetadata(allocator, knownMimeType, tags);
  }

  /**
   * create tagging content
   *
   * @param allocator the {@link ByteBufAllocator} to use to create intermediate buffers as needed.
   * @param tags tag values
   * @return tagging content
   */
  public static ByteBuf createTaggingContent(ByteBufAllocator allocator, Collection<String> tags) {
    return TaggingMetadataCodec.createTaggingContent(allocator, tags);
  }
}
