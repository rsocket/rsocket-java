/*
 * Copyright 2015-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
