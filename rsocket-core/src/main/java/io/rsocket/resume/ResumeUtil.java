package io.rsocket.resume;

import io.rsocket.Frame;
import io.rsocket.FrameType;
import io.rsocket.frame.FrameHeaderFlyweight;

public class ResumeUtil {
  public static boolean isTracked(FrameType frameType) {
    switch (frameType) {
      case REQUEST_CHANNEL:
      case REQUEST_STREAM:
      case REQUEST_RESPONSE:
      case FIRE_AND_FORGET:
        // case METADATA_PUSH:
      case REQUEST_N:
      case CANCEL:
      case ERROR:
      case PAYLOAD:
        return true;
      default:
        return false;
    }
  }

  public static boolean isTracked(Frame frame) {
    return isTracked(frame.getType());
  }

  public static int offset(Frame frame) {
    int length = frame.content().readableBytes();

    if (length < FrameHeaderFlyweight.FRAME_HEADER_LENGTH) {
      throw new IllegalStateException("invalid frame");
    }

    return length - FrameHeaderFlyweight.FRAME_LENGTH_SIZE;
  }
}
