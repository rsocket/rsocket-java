package io.rsocket.frame.decoder;

import io.netty.buffer.ByteBuf;
import io.rsocket.Payload;
import io.rsocket.frame.PayloadFrameFlyweight;
import io.rsocket.util.ByteBufPayload;

/**
 * Frame decoder that decodes a frame to a payload without copying. The caller is responsible for
 * for releasing the payload to free memory when they no long need it.
 */
public class ZeroCopyFrameDecoder implements FrameDecoder {
  @Override
  public Payload apply(ByteBuf byteBuf) {
    ByteBuf metadata = PayloadFrameFlyweight.metadata(byteBuf).retain();
    ByteBuf data = PayloadFrameFlyweight.data(byteBuf).retain();
    return ByteBufPayload.create(data, metadata);
  }
}
