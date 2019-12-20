package io.rsocket.metadata.security;

import io.netty.buffer.ByteBuf;
import io.rsocket.metadata.CompositeMetadata;
import io.rsocket.metadata.WellKnownMimeType;

public class AuthMetadata implements CompositeMetadata.Entry {

  final CompositeMetadata.Entry source;

  public AuthMetadata(CompositeMetadata.Entry source) {
    this.source = source;
  }

  @Override
  public ByteBuf getContent() {
    return source.getContent();
  }

  @Override
  public String getMimeType() {
    return WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION.getString();
  }
}
