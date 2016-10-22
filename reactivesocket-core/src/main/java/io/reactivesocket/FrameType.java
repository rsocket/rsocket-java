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

/**
 * Types of {@link Frame} that can be sent.
 */
public enum FrameType {
    // blank type that is not defined
    UNDEFINED(0x00),
    // Connection
    SETUP(0x01, Flags.CAN_HAVE_METADATA_AND_DATA),
    LEASE(0x02, Flags.CAN_HAVE_METADATA),
    KEEPALIVE(0x03, Flags.CAN_HAVE_DATA),
    // Requester to start request
    REQUEST_RESPONSE(0x04, Flags.CAN_HAVE_METADATA_AND_DATA | Flags.IS_REQUEST_TYPE),
    FIRE_AND_FORGET(0x05, Flags.CAN_HAVE_METADATA_AND_DATA | Flags.IS_REQUEST_TYPE),
    REQUEST_STREAM(0x06, Flags.CAN_HAVE_METADATA_AND_DATA | Flags.IS_REQUEST_TYPE | Flags.HAS_INITIAL_REQUEST_N),
    REQUEST_SUBSCRIPTION(0x07, Flags.CAN_HAVE_METADATA_AND_DATA | Flags.IS_REQUEST_TYPE | Flags.HAS_INITIAL_REQUEST_N),
    REQUEST_CHANNEL(0x08, Flags.CAN_HAVE_METADATA_AND_DATA | Flags.IS_REQUEST_TYPE | Flags.HAS_INITIAL_REQUEST_N),
    // Requester mid-stream
    REQUEST_N(0x09),
    CANCEL(0x0A, Flags.CAN_HAVE_METADATA),
    // Responder
    RESPONSE(0x0B, Flags.CAN_HAVE_METADATA_AND_DATA),
    ERROR(0x0C, Flags.CAN_HAVE_METADATA_AND_DATA),
    // Requester & Responder
    METADATA_PUSH(0x0D, Flags.CAN_HAVE_METADATA),
    // synthetic types from Responder for use by the rest of the machinery
    NEXT(0x0E, Flags.CAN_HAVE_METADATA_AND_DATA),
    COMPLETE(0x0F),
    NEXT_COMPLETE(0x10, Flags.CAN_HAVE_METADATA_AND_DATA),
    EXT(0xFFFF, Flags.CAN_HAVE_METADATA_AND_DATA);

    private static class Flags {
        private Flags() {}

        private static final int CAN_HAVE_DATA = 0b0001;
        private static final int CAN_HAVE_METADATA = 0b0010;
        private static final int CAN_HAVE_METADATA_AND_DATA = 0b0011;
        private static final int IS_REQUEST_TYPE = 0b0100;
        private static final int HAS_INITIAL_REQUEST_N = 0b1000;
    }

    private static FrameType[] typesById;

    private final int id;
    private final int flags;

    /**
     * Index types by id for indexed lookup. 
     */
    static {
        int max = 0;

        for (FrameType t : values()) {
            max = Math.max(t.id, max);
        }

        typesById = new FrameType[max + 1];

        for (FrameType t : values()) {
            typesById[t.id] = t;
        }
    }

    FrameType(final int id) {
        this(id, 0);
    }

    FrameType(int id, int flags) {
        this.id = id;
        this.flags = flags;
    }
    
    public int getEncodedType() {
        return id;
    }

    public boolean isRequestType() {
        return Flags.IS_REQUEST_TYPE == (flags & Flags.IS_REQUEST_TYPE);
    }

    public boolean hasInitialRequestN() {
        return Flags.HAS_INITIAL_REQUEST_N == (flags & Flags.HAS_INITIAL_REQUEST_N);
    }

    public boolean canHaveData() {
        return Flags.CAN_HAVE_DATA == (flags & Flags.CAN_HAVE_DATA);
    }

    public boolean canHaveMetadata() {
        return Flags.CAN_HAVE_METADATA == (flags & Flags.CAN_HAVE_METADATA);
    }

    // TODO: offset of metadata and data (simplify parsing) naming: endOfFrameHeaderOffset()
    public int payloadOffset() {
        return 0;
    }

    public static FrameType from(int id) {
        return typesById[id];
    }
}