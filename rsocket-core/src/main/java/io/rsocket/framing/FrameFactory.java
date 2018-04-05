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

package io.rsocket.framing;

import static io.rsocket.framing.CancelFrame.createCancelFrame;
import static io.rsocket.framing.ErrorFrame.createErrorFrame;
import static io.rsocket.framing.ExtensionFrame.createExtensionFrame;
import static io.rsocket.framing.Frame.FRAME_TYPE_SHIFT;
import static io.rsocket.framing.KeepaliveFrame.createKeepaliveFrame;
import static io.rsocket.framing.LeaseFrame.createLeaseFrame;
import static io.rsocket.framing.MetadataPushFrame.createMetadataPushFrame;
import static io.rsocket.framing.PayloadFrame.createPayloadFrame;
import static io.rsocket.framing.RequestChannelFrame.createRequestChannelFrame;
import static io.rsocket.framing.RequestFireAndForgetFrame.createRequestFireAndForgetFrame;
import static io.rsocket.framing.RequestNFrame.createRequestNFrame;
import static io.rsocket.framing.RequestResponseFrame.createRequestResponseFrame;
import static io.rsocket.framing.RequestStreamFrame.createRequestStreamFrame;
import static io.rsocket.framing.ResumeFrame.createResumeFrame;
import static io.rsocket.framing.ResumeOkFrame.createResumeOkFrame;
import static io.rsocket.framing.SetupFrame.createSetupFrame;

import io.netty.buffer.ByteBuf;
import java.util.Objects;

/**
 * A factory for creating RSocket frames from {@link ByteBuf}s.
 *
 * @see <a href="https://github.com/rsocket/rsocket/blob/master/Protocol.md#frame-types">Frame
 *     Types</a>
 */
public final class FrameFactory {

  private FrameFactory() {}

  /**
   * Returns a strongly-type {@link Frame} created from a {@link ByteBuf}.
   *
   * @param byteBuf the {@code ByteBuf} to create the {@link Frame} from
   * @return the strongly-typed {@link Frame}
   * @throws NullPointerException if {@code byteBuf} is {@code null}
   */
  public static Frame createFrame(ByteBuf byteBuf) {
    Objects.requireNonNull(byteBuf, "byteBuf must not be null");

    FrameType frameType = getFrameType(byteBuf);
    switch (frameType) {
      case SETUP:
        return createSetupFrame(byteBuf);
      case LEASE:
        return createLeaseFrame(byteBuf);
      case KEEPALIVE:
        return createKeepaliveFrame(byteBuf);
      case REQUEST_RESPONSE:
        return createRequestResponseFrame(byteBuf);
      case REQUEST_FNF:
        return createRequestFireAndForgetFrame(byteBuf);
      case REQUEST_STREAM:
        return createRequestStreamFrame(byteBuf);
      case REQUEST_CHANNEL:
        return createRequestChannelFrame(byteBuf);
      case REQUEST_N:
        return createRequestNFrame(byteBuf);
      case CANCEL:
        return createCancelFrame(byteBuf);
      case PAYLOAD:
        return createPayloadFrame(byteBuf);
      case ERROR:
        return createErrorFrame(byteBuf);
      case METADATA_PUSH:
        return createMetadataPushFrame(byteBuf);
      case RESUME:
        return createResumeFrame(byteBuf);
      case RESUME_OK:
        return createResumeOkFrame(byteBuf);
      case EXT:
        return createExtensionFrame(byteBuf);
      default:
        throw new IllegalArgumentException(
            String.format("Cannot create frame for type %s", frameType));
    }
  };

  private static FrameType getFrameType(ByteBuf byteBuf) {
    int encodedType = byteBuf.getUnsignedShort(0) >> FRAME_TYPE_SHIFT;
    return FrameType.fromEncodedType(encodedType);
  }
}
