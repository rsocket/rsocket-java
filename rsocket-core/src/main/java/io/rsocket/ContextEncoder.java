package io.rsocket;

import reactor.util.context.Context;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

public interface ContextEncoder {
  ContextEncoder NONE = new ContextEncoder() {
    @Override public Payload encode(Payload payload, Context context) {
      return payload;
    }

    @Override public Tuple2<Payload, Context> decode(Payload payload) {
      return Tuples.of(payload, Context.empty());
    }
  };

  static ContextEncoder forMimeType(String mimeType) {
    if (mimeType.equals("applicationJson")) {
      return new JsonContextEncoder();
    }

    return ContextEncoder.NONE;
  }

  Payload encode(Payload payload, Context context);

  Tuple2<Payload, Context> decode(Payload payload);
}
