package io.rsocket.frame.decoder;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.Payload;
import io.rsocket.frame.*;
import io.rsocket.util.ByteBufPayload;

import java.nio.ByteBuffer;

/** Default Frame decoder that copies the frames contents for easy of use. */
class DefaultFrameDecoder implements FrameDecoder {
  
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

    ByteBuffer metadata = ByteBuffer.allocateDirect(m.readableBytes());
    ByteBuffer data = ByteBuffer.allocateDirect(d.readableBytes());

    data.put(d.nioBuffer());
    data.flip();
    metadata.put(m.nioBuffer());
    metadata.flip();

    return ByteBufPayload.create(data, metadata);
  }
}
