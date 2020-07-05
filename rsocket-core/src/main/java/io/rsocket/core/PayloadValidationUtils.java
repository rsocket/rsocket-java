package io.rsocket.core;

import static io.rsocket.frame.FrameLengthCodec.FRAME_LENGTH_MASK;

import io.rsocket.Payload;
import io.rsocket.frame.FrameHeaderCodec;
import io.rsocket.frame.FrameLengthCodec;

final class PayloadValidationUtils {
  static final String INVALID_PAYLOAD_ERROR_MESSAGE =
      "The payload is too big to send as a single frame with a 24-bit encoded length. Consider enabling fragmentation via RSocketFactory.";

  static boolean isValid(int mtu, Payload payload, int maxFrameLength) {
    if (mtu > 0) {
      return true;
    }

    if (payload.hasMetadata()) {
      return ((FrameHeaderCodec.size()
              + FrameLengthCodec.FRAME_LENGTH_SIZE
              + FrameHeaderCodec.size()
              + payload.data().readableBytes()
              + payload.metadata().readableBytes())
          <= maxFrameLength);
    } else {
      return ((FrameHeaderCodec.size()
              + payload.data().readableBytes()
              + FrameLengthCodec.FRAME_LENGTH_SIZE)
          <= maxFrameLength);
    }
  }

  static void assertValidateSetup(int maxFrameLength, int maxInboundPayloadSize, int mtu) {

    if (maxFrameLength > FRAME_LENGTH_MASK) {
      throw new IllegalArgumentException(
          "Configured maxFrameLength["
              + maxFrameLength
              + "] exceeds maxFrameLength limit "
              + FRAME_LENGTH_MASK);
    }

    if (maxFrameLength > maxInboundPayloadSize) {
      throw new IllegalArgumentException(
          "Configured maxFrameLength["
              + maxFrameLength
              + "] exceeds maxPayloadSize["
              + maxInboundPayloadSize
              + "]");
    }

    if (mtu != 0 && mtu > maxFrameLength) {
      throw new IllegalArgumentException(
          "Configured maximumTransmissionUnit["
              + mtu
              + "] exceeds configured maxFrameLength["
              + maxFrameLength
              + "]");
    }
  }
}
