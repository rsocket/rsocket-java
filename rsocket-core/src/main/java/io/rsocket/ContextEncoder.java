package io.rsocket;

import reactor.util.context.Context;

@FunctionalInterface
public interface ContextEncoder {
  ContextEncoder NONE = ((payload, context) -> payload);

  static ContextEncoder forMimeType(String mimeType) {
    if (mimeType.equals("applicationJson")) {
      return new JsonContextEncoder();
    }

    return ContextEncoder.NONE;
  }

  Payload apply(Payload payload, Context context);
}
