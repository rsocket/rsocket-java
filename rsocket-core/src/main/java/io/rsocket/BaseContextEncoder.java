package io.rsocket;

import io.rsocket.util.PayloadImpl;
import java.io.IOException;
import java.nio.ByteBuffer;
import reactor.util.context.Context;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

public abstract class BaseContextEncoder implements ContextEncoder {
  @Override public Payload encode(Payload payload, Context context) {
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

  @Override public Tuple2<Payload, Context> decode(Payload payload) {
    try {
      if (payload.hasMetadata()) {
        Context contextMetadata = decode(payload.getMetadata());
        return Tuples.of(new PayloadImpl(payload.getData()), contextMetadata);
      } else {
        return Tuples.of(payload, Context.empty());
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  protected abstract ByteBuffer encode(Context context) throws IOException;

  protected abstract Context decode(ByteBuffer data) throws IOException;
}
