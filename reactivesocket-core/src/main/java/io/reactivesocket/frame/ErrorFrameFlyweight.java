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
package io.reactivesocket.frame;

import io.reactivesocket.FrameType;
import io.reactivesocket.exceptions.ApplicationException;
import io.reactivesocket.exceptions.CancelException;
import io.reactivesocket.exceptions.ConnectionException;
import io.reactivesocket.exceptions.InvalidRequestException;
import io.reactivesocket.exceptions.InvalidSetupException;
import io.reactivesocket.exceptions.RejectedException;
import io.reactivesocket.exceptions.RejectedSetupException;
import io.reactivesocket.exceptions.UnsupportedSetupException;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

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
    public static final int CANCEL = 0x00000203;
    public static final int INVALID = 0x00000204;

    // relative to start of passed offset
    private static final int ERROR_CODE_FIELD_OFFSET = FrameHeaderFlyweight.FRAME_HEADER_LENGTH;
    private static final int PAYLOAD_OFFSET = ERROR_CODE_FIELD_OFFSET + BitUtil.SIZE_OF_INT;

    public static int computeFrameLength(
        final int metadataLength,
        final int dataLength
    ) {
        int length = FrameHeaderFlyweight.computeFrameHeaderLength(
            FrameType.ERROR, metadataLength, dataLength);
        return length + BitUtil.SIZE_OF_INT;
    }

    public static int encode(
        final MutableDirectBuffer mutableDirectBuffer,
        final int offset,
        final int streamId,
        final int errorCode,
        final ByteBuffer metadata,
        final ByteBuffer data
    ) {
        final int frameLength = computeFrameLength(metadata.remaining(), data.remaining());

        int length = FrameHeaderFlyweight.encodeFrameHeader(
            mutableDirectBuffer, offset, frameLength, 0, FrameType.ERROR, streamId);

        mutableDirectBuffer.putInt(
            offset + ERROR_CODE_FIELD_OFFSET, errorCode, ByteOrder.BIG_ENDIAN);
        length += BitUtil.SIZE_OF_INT;

        length += FrameHeaderFlyweight.encodeMetadata(
            mutableDirectBuffer, offset, offset + length, metadata);
        length += FrameHeaderFlyweight.encodeData(mutableDirectBuffer, offset + length, data);

        return length;
    }

    public static int errorCodeFromException(Throwable ex) {
        if (ex instanceof InvalidSetupException) {
            return INVALID_SETUP;
        } else if (ex instanceof UnsupportedSetupException) {
            return UNSUPPORTED_SETUP;
        } else if (ex instanceof RejectedSetupException) {
            return REJECTED_SETUP;
        } else if (ex instanceof ConnectionException) {
            return CONNECTION_ERROR;
        } else if (ex instanceof InvalidRequestException) {
            return INVALID;
        } else if (ex instanceof ApplicationException) {
            return APPLICATION_ERROR;
        } else if (ex instanceof RejectedException) {
            return REJECTED;
        } else if (ex instanceof CancelException) {
            return CANCEL;
        }
        return INVALID;
    }

    public static int errorCode(final DirectBuffer directBuffer, final int offset) {
        return directBuffer.getInt(offset + ERROR_CODE_FIELD_OFFSET, ByteOrder.BIG_ENDIAN);
    }

    public static int payloadOffset(final DirectBuffer directBuffer, final int offset) {
        return offset + FrameHeaderFlyweight.FRAME_HEADER_LENGTH + BitUtil.SIZE_OF_INT;
    }
}
