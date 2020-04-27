package io.rsocket.core;

import io.rsocket.Payload;
import io.rsocket.frame.FrameHeaderFlyweight;
import io.rsocket.frame.FrameLengthFlyweight;

final class PayloadValidationUtils {
  static final String INVALID_PAYLOAD_ERROR_MESSAGE =
      "The payload is too big to send as a single frame with a 24-bit encoded length. Consider enabling fragmentation via RSocketFactory.";

  static boolean isValid(int mtu, Payload payload) {
    if (mtu > 0) {
      return true;
    }

    if (payload.hasMetadata()) {
      return (((FrameHeaderFlyweight.size()
                  + FrameLengthFlyweight.FRAME_LENGTH_SIZE
                  + FrameHeaderFlyweight.size()
                  + payload.data().readableBytes()
                  + payload.metadata().readableBytes())
              & ~FrameLengthFlyweight.FRAME_LENGTH_MASK)
          == 0);
    } else {
      return (((FrameHeaderFlyweight.size()
                  + payload.data().readableBytes()
                  + FrameLengthFlyweight.FRAME_LENGTH_SIZE)
              & ~FrameLengthFlyweight.FRAME_LENGTH_MASK)
          == 0);
    }
  }
}
