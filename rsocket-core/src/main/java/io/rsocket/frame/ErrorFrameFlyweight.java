/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.rsocket.FrameType;
import io.rsocket.exceptions.*;
import java.nio.charset.StandardCharsets;

public class ErrorFrameFlyweight {

  private ErrorFrameFlyweight() {}

  // defined error codes
  public static final int INVALID_SETUP = 0x00000001;
  public static final int UNSUPPORTED_SETUP = 0x00000002;
  public static final int REJECTED_SETUP = 0x00000003;
  public static final int REJECTED_RESUME = 0x00000004;
  public static final int CONNECTION_ERROR = 0x00000101;
  public static final int CONNECTION_CLOSE = 0x00000102;
  public static final int APPLICATION_ERROR = 0x00000201;
  public static final int REJECTED = 0x00000202;
  public static final int CANCELED = 0x00000203;
  public static final int INVALID = 0x00000204;

  // relative to start of passed offset
  private static final int ERROR_CODE_FIELD_OFFSET = FrameHeaderFlyweight.FRAME_HEADER_LENGTH;
  private static final int PAYLOAD_OFFSET = ERROR_CODE_FIELD_OFFSET + Integer.BYTES;

  public static int computeFrameLength(final int dataLength) {
    int length = FrameHeaderFlyweight.computeFrameHeaderLength(FrameType.ERROR, null, dataLength);
    return length + Integer.BYTES;
  }

  public static int encode(
      final ByteBuf byteBuf, final int streamId, final int errorCode, final ByteBuf data) {
    final int frameLength = computeFrameLength(data.readableBytes());

    int length =
        FrameHeaderFlyweight.encodeFrameHeader(byteBuf, frameLength, 0, FrameType.ERROR, streamId);

    byteBuf.setInt(ERROR_CODE_FIELD_OFFSET, errorCode);
    length += Integer.BYTES;

    length += FrameHeaderFlyweight.encodeData(byteBuf, length, data);

    return length;
  }

  public static int errorCodeFromException(Throwable ex) {
    if (ex instanceof RSocketException) {
      return ((RSocketException) ex).errorCode();
    }

    return APPLICATION_ERROR;
  }

  public static int errorCode(final ByteBuf byteBuf) {
    return byteBuf.getInt(ERROR_CODE_FIELD_OFFSET);
  }

  public static int payloadOffset(final ByteBuf byteBuf) {
    return FrameHeaderFlyweight.FRAME_HEADER_LENGTH + Integer.BYTES;
  }

  public static String message(ByteBuf content) {
    return FrameHeaderFlyweight.sliceFrameData(content).toString(StandardCharsets.UTF_8);
  }
}
