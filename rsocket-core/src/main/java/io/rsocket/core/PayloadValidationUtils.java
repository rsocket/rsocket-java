package io.rsocket.core;

import static io.rsocket.core.FragmentationUtils.FRAME_OFFSET;
import static io.rsocket.core.FragmentationUtils.FRAME_OFFSET_WITH_INITIAL_REQUEST_N;
import static io.rsocket.core.FragmentationUtils.FRAME_OFFSET_WITH_METADATA;
import static io.rsocket.core.FragmentationUtils.FRAME_OFFSET_WITH_METADATA_AND_INITIAL_REQUEST_N;
import static io.rsocket.frame.FrameLengthCodec.FRAME_LENGTH_MASK;

import io.netty.buffer.ByteBuf;
import io.rsocket.Payload;

final class PayloadValidationUtils {
  static final String INVALID_PAYLOAD_ERROR_MESSAGE =
      "The payload is too big to be send as a single frame with a max frame length %s. Consider enabling fragmentation.";

  static boolean isValid(int mtu, int maxFrameLength, Payload payload, boolean hasInitialRequestN) {

    if (mtu > 0) {
      return true;
    }

    final boolean hasMetadata = payload.hasMetadata();
    final ByteBuf data = payload.data();

    int unitSize;
    if (hasMetadata) {
      final ByteBuf metadata = payload.metadata();
      unitSize =
          (hasInitialRequestN
                  ? FRAME_OFFSET_WITH_METADATA_AND_INITIAL_REQUEST_N
                  : FRAME_OFFSET_WITH_METADATA)
              + metadata.readableBytes()
              + // metadata payload bytes
              data.readableBytes(); // data payload bytes
    } else {
      unitSize =
          (hasInitialRequestN ? FRAME_OFFSET_WITH_INITIAL_REQUEST_N : FRAME_OFFSET)
              + data.readableBytes(); // data payload bytes
    }

    return unitSize <= maxFrameLength;
  }

  static boolean isValidMetadata(int maxFrameLength, ByteBuf metadata) {
    return FRAME_OFFSET + metadata.readableBytes() <= maxFrameLength;
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
