package io.rsocket.frame.decoder;

import io.netty.buffer.ByteBuf;
import io.rsocket.Payload;
import java.util.function.Function;

public interface PayloadDecoder extends Function<ByteBuf, Payload> {
  PayloadDecoder DEFAULT = new DefaultPayloadDecoder();
  PayloadDecoder ZERO_COPY = new ZeroCopyPayloadDecoder();
}
