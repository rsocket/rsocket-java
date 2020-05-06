package io.rsocket.core;

import io.rsocket.Payload;
import io.rsocket.frame.FrameHeaderCodec;
import io.rsocket.frame.FrameLengthCodec;

final class PayloadValidationUtils {
  static final String INVALID_PAYLOAD_ERROR_MESSAGE =
      "The payload is too big to send as a single frame with a 24-bit encoded length. Consider enabling fragmentation via RSocketFactory.";

  static boolean isValid(int mtu, Payload payload) {
    if (mtu > 0) {
      return true;
    }

    if (payload.hasMetadata()) {
      return (((FrameHeaderCodec.size()
                  + FrameLengthCodec.FRAME_LENGTH_SIZE
                  + FrameHeaderCodec.size()
                  + payload.data().readableBytes()
                  + payload.metadata().readableBytes())
              & ~FrameLengthCodec.FRAME_LENGTH_MASK)
          == 0);
    } else {
      return (((FrameHeaderCodec.size()
                  + payload.data().readableBytes()
                  + FrameLengthCodec.FRAME_LENGTH_SIZE)
              & ~FrameLengthCodec.FRAME_LENGTH_MASK)
          == 0);
    }
  }
}
