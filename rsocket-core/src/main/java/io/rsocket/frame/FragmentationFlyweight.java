package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import reactor.util.annotation.Nullable;

/** FragmentationFlyweight is used to re-assemble frames */
public class FragmentationFlyweight {
  public static ByteBuf encode(final ByteBufAllocator allocator, ByteBuf header, ByteBuf data) {
    return encode(allocator, header, null, data);
  }

  public static ByteBuf encode(
      final ByteBufAllocator allocator, ByteBuf header, @Nullable ByteBuf metadata, ByteBuf data) {

    final boolean hasData = data != null && data.isReadable();
    final boolean hasMetadata = metadata != null && metadata.isReadable();

    if (hasData && hasMetadata) {
      return DataAndMetadataFlyweight.encode(allocator, header, metadata, data);
    } else if (hasMetadata) {
      return DataAndMetadataFlyweight.encode(allocator, header, metadata);
    } else if (hasData) {
      return DataAndMetadataFlyweight.encodeOnlyData(allocator, header, data);
    } else {
      return header;
    }
  }
}
