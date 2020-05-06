package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;

public class FrameUtil {

  private FrameUtil() {}

  public static String toString(ByteBuf frame) {
    FrameType frameType = FrameHeaderCodec.frameType(frame);
    int streamId = FrameHeaderCodec.streamId(frame);
    StringBuilder payload = new StringBuilder();

    payload
        .append("\nFrame => Stream ID: ")
        .append(streamId)
        .append(" Type: ")
        .append(frameType)
        .append(" Flags: 0b")
        .append(Integer.toBinaryString(FrameHeaderCodec.flags(frame)))
        .append(" Length: " + frame.readableBytes());

    if (frameType.hasInitialRequestN()) {
      payload.append(" InitialRequestN: ").append(RequestStreamFrameCodec.initialRequestN(frame));
    }

    if (frameType == FrameType.REQUEST_N) {
      payload.append(" RequestN: ").append(RequestNFrameCodec.requestN(frame));
    }

    if (FrameHeaderCodec.hasMetadata(frame)) {
      payload.append("\nMetadata:\n");

      ByteBufUtil.appendPrettyHexDump(payload, getMetadata(frame, frameType));
    }

    payload.append("\nData:\n");
    ByteBufUtil.appendPrettyHexDump(payload, getData(frame, frameType));

    return payload.toString();
  }

  private static ByteBuf getMetadata(ByteBuf frame, FrameType frameType) {
    boolean hasMetadata = FrameHeaderCodec.hasMetadata(frame);
    if (hasMetadata) {
      ByteBuf metadata;
      switch (frameType) {
        case REQUEST_FNF:
          metadata = RequestFireAndForgetFrameCodec.metadata(frame);
          break;
        case REQUEST_STREAM:
          metadata = RequestStreamFrameCodec.metadata(frame);
          break;
        case REQUEST_RESPONSE:
          metadata = RequestResponseFrameCodec.metadata(frame);
          break;
        case REQUEST_CHANNEL:
          metadata = RequestChannelFrameCodec.metadata(frame);
          break;
          // Payload and synthetic types
        case PAYLOAD:
        case NEXT:
        case NEXT_COMPLETE:
        case COMPLETE:
          metadata = PayloadFrameCodec.metadata(frame);
          break;
        case METADATA_PUSH:
          metadata = MetadataPushFrameCodec.metadata(frame);
          break;
        case SETUP:
          metadata = SetupFrameCodec.metadata(frame);
          break;
        case LEASE:
          metadata = LeaseFrameCodec.metadata(frame);
          break;
        default:
          return Unpooled.EMPTY_BUFFER;
      }
      return metadata;
    } else {
      return Unpooled.EMPTY_BUFFER;
    }
  }

  private static ByteBuf getData(ByteBuf frame, FrameType frameType) {
    ByteBuf data;
    switch (frameType) {
      case REQUEST_FNF:
        data = RequestFireAndForgetFrameCodec.data(frame);
        break;
      case REQUEST_STREAM:
        data = RequestStreamFrameCodec.data(frame);
        break;
      case REQUEST_RESPONSE:
        data = RequestResponseFrameCodec.data(frame);
        break;
      case REQUEST_CHANNEL:
        data = RequestChannelFrameCodec.data(frame);
        break;
        // Payload and synthetic types
      case PAYLOAD:
      case NEXT:
      case NEXT_COMPLETE:
      case COMPLETE:
        data = PayloadFrameCodec.data(frame);
        break;
      case SETUP:
        data = SetupFrameCodec.data(frame);
        break;
      default:
        return Unpooled.EMPTY_BUFFER;
    }
    return data;
  }
}
