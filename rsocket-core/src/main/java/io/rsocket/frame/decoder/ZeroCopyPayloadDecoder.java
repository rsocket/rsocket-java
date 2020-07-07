package io.rsocket.frame.decoder;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.Payload;
import io.rsocket.frame.FrameHeaderCodec;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.MetadataPushFrameCodec;
import io.rsocket.frame.PayloadFrameCodec;
import io.rsocket.frame.RequestChannelFrameCodec;
import io.rsocket.frame.RequestFireAndForgetFrameCodec;
import io.rsocket.frame.RequestResponseFrameCodec;
import io.rsocket.frame.RequestStreamFrameCodec;
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
    FrameType type = FrameHeaderCodec.frameType(byteBuf);
    switch (type) {
      case REQUEST_FNF:
        d = RequestFireAndForgetFrameCodec.data(byteBuf);
        m = RequestFireAndForgetFrameCodec.metadata(byteBuf);
        break;
      case REQUEST_RESPONSE:
        d = RequestResponseFrameCodec.data(byteBuf);
        m = RequestResponseFrameCodec.metadata(byteBuf);
        break;
      case REQUEST_STREAM:
        d = RequestStreamFrameCodec.data(byteBuf);
        m = RequestStreamFrameCodec.metadata(byteBuf);
        break;
      case REQUEST_CHANNEL:
      case REQUEST_CHANNEL_COMPLETE:
        d = RequestChannelFrameCodec.data(byteBuf);
        m = RequestChannelFrameCodec.metadata(byteBuf);
        break;
      case NEXT:
      case NEXT_COMPLETE:
        d = PayloadFrameCodec.data(byteBuf);
        m = PayloadFrameCodec.metadata(byteBuf);
        break;
      case METADATA_PUSH:
        d = Unpooled.EMPTY_BUFFER;
        m = MetadataPushFrameCodec.metadata(byteBuf);
        break;
      default:
        throw new IllegalArgumentException("unsupported frame type: " + type);
    }

    return ByteBufPayload.create(d.retain(), m != null ? m.retain() : null);
  }
}
