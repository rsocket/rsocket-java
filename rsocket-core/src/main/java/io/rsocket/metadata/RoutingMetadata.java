package io.rsocket.metadata;

import io.netty.buffer.ByteBuf;

/**
 * Routing Metadata extension from
 * https://github.com/rsocket/rsocket/blob/master/Extensions/Routing.md
 *
 * @author linux_china
 */
public class RoutingMetadata extends TaggingMetadata {
  private static final WellKnownMimeType ROUTING_MIME_TYPE =
      WellKnownMimeType.MESSAGE_RSOCKET_ROUTING;

  public RoutingMetadata(ByteBuf content) {
    super(ROUTING_MIME_TYPE.getString(), content);
  }
}
