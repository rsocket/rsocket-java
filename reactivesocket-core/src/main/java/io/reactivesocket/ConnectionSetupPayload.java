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
package io.reactivesocket;

import io.reactivesocket.frame.SetupFrameFlyweight;

import java.nio.ByteBuffer;

/**
 * Exposed to server for determination of RequestHandler based on mime types and SETUP metadata/data
 */
public abstract class ConnectionSetupPayload implements Payload {

    public static final int NO_FLAGS = 0;
    public static final int HONOR_LEASE = SetupFrameFlyweight.FLAGS_WILL_HONOR_LEASE;
    public static final int STRICT_INTERPRETATION = SetupFrameFlyweight.FLAGS_STRICT_INTERPRETATION;

    public static ConnectionSetupPayload create(String metadataMimeType, String dataMimeType) {
        return new ConnectionSetupPayloadImpl(metadataMimeType, dataMimeType, Frame.NULL_BYTEBUFFER,
                                              Frame.NULL_BYTEBUFFER, NO_FLAGS);
    }

    public static ConnectionSetupPayload create(String metadataMimeType, String dataMimeType, Payload payload) {
        return new ConnectionSetupPayloadImpl(metadataMimeType, dataMimeType, payload.getData(),
                                              payload.getMetadata(), NO_FLAGS);
    }

    public static ConnectionSetupPayload create(String metadataMimeType, String dataMimeType, int flags) {
        return new ConnectionSetupPayloadImpl(metadataMimeType, dataMimeType, Frame.NULL_BYTEBUFFER,
                                              Frame.NULL_BYTEBUFFER, flags);
    }

    public static ConnectionSetupPayload create(final Frame setupFrame) {
        Frame.ensureFrameType(FrameType.SETUP, setupFrame);
        return new ConnectionSetupPayloadImpl(Frame.Setup.metadataMimeType(setupFrame),
                                              Frame.Setup.dataMimeType(setupFrame),
                                              setupFrame.getData(), setupFrame.getMetadata(),
                                              Frame.Setup.getFlags(setupFrame));
    }

    public abstract String metadataMimeType();

    public abstract String dataMimeType();

    public int getFlags() {
        return HONOR_LEASE;
    }

    public boolean willClientHonorLease() {
        return HONOR_LEASE == (getFlags() & HONOR_LEASE);
    }

    public boolean doesClientRequestStrictInterpretation() {
        return STRICT_INTERPRETATION == (getFlags() & STRICT_INTERPRETATION);
    }

    private static final class ConnectionSetupPayloadImpl extends ConnectionSetupPayload {

        private final String metadataMimeType;
        private final String dataMimeType;
        private final ByteBuffer data;
        private final ByteBuffer metadata;
        private final int flags;

        public ConnectionSetupPayloadImpl(String metadataMimeType, String dataMimeType, ByteBuffer data,
                                          ByteBuffer metadata, int flags) {
            this.metadataMimeType = metadataMimeType;
            this.dataMimeType = dataMimeType;
            this.data = data;
            this.metadata = metadata;
            this.flags = flags;
        }

        @Override
        public String metadataMimeType() {
            return metadataMimeType;
        }

        @Override
        public String dataMimeType() {
            return dataMimeType;
        }

        @Override
        public ByteBuffer getData() {
            return data;
        }

        @Override
        public ByteBuffer getMetadata() {
            return metadata;
        }

        @Override
        public int getFlags() {
            return flags;
        }
    }
}
