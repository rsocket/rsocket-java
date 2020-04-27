package io.rsocket.frame.decoder;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.Payload;
import io.rsocket.frame.FrameHeaderFlyweight;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.MetadataPushFrameFlyweight;
import io.rsocket.frame.PayloadFrameFlyweight;
import io.rsocket.frame.RequestChannelFrameFlyweight;
import io.rsocket.frame.RequestFireAndForgetFrameFlyweight;
import io.rsocket.frame.RequestResponseFrameFlyweight;
import io.rsocket.frame.RequestStreamFrameFlyweight;
import io.rsocket.util.ByteBufPayload;

/**
 * Frame decoder that decodes a frame to a payload without copying. The caller is responsible for
 * for releasing the payload to free memory when they no long need it.
 */
public class ZeroCopyPayloadDecoder implements PayloadDecoder {
  @Override
  public Payload apply(ByteBuf byteBuf) {
    ByteBuf m;
    ByteBuf d;
    FrameType type = FrameHeaderFlyweight.frameType(byteBuf);
    switch (type) {
      case REQUEST_FNF:
        d = RequestFireAndForgetFrameFlyweight.data(byteBuf);
        m = RequestFireAndForgetFrameFlyweight.metadata(byteBuf);
        break;
      case REQUEST_RESPONSE:
        d = RequestResponseFrameFlyweight.data(byteBuf);
        m = RequestResponseFrameFlyweight.metadata(byteBuf);
        break;
      case REQUEST_STREAM:
        d = RequestStreamFrameFlyweight.data(byteBuf);
        m = RequestStreamFrameFlyweight.metadata(byteBuf);
        break;
      case REQUEST_CHANNEL:
        d = RequestChannelFrameFlyweight.data(byteBuf);
        m = RequestChannelFrameFlyweight.metadata(byteBuf);
        break;
      case NEXT:
      case NEXT_COMPLETE:
        d = PayloadFrameFlyweight.data(byteBuf);
        m = PayloadFrameFlyweight.metadata(byteBuf);
        break;
      case METADATA_PUSH:
        d = Unpooled.EMPTY_BUFFER;
        m = MetadataPushFrameFlyweight.metadata(byteBuf);
        break;
      default:
        throw new IllegalArgumentException("unsupported frame type: " + type);
    }

    return ByteBufPayload.create(d.retain(), m != null ? m.retain() : null);
  }
}
