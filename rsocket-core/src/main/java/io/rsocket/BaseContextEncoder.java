package io.rsocket;

import io.rsocket.util.PayloadImpl;
import java.io.IOException;
import java.nio.ByteBuffer;
import reactor.util.context.Context;

public abstract class BaseContextEncoder implements ContextEncoder {
  @Override public Payload apply(Payload payload, Context context) {
    try {
      if (!payload.hasMetadata()) {
        ByteBuffer contextMetadata = encode(context);
        return new PayloadImpl(payload.getData(), contextMetadata);
      } else {
        return payload;
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  protected abstract ByteBuffer encode(Context context) throws IOException;
}
