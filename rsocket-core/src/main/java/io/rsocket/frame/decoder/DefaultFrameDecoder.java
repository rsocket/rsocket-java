package io.rsocket.frame.decoder;

import io.netty.buffer.ByteBuf;
import io.rsocket.Payload;
import io.rsocket.frame.PayloadFrameFlyweight;
import io.rsocket.util.ByteBufPayload;

import java.nio.ByteBuffer;

/** Default Frame decoder that copies the frames contents for easy of use. */
public class DefaultFrameDecoder implements FrameDecoder {

  @Override
  public Payload apply(ByteBuf byteBuf) {
    ByteBuf m = PayloadFrameFlyweight.metadata(byteBuf);
    ByteBuf d = PayloadFrameFlyweight.data(byteBuf);
    
    ByteBuffer metadata = ByteBuffer.allocateDirect(m.readableBytes());
    ByteBuffer data = ByteBuffer.allocateDirect(d.readableBytes());

    m.writeBytes(metadata);
    d.writeBytes(data);

    return ByteBufPayload.create(data, metadata);
  }
}
