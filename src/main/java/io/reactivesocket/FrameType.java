/**
 * Copyright 2015 Netflix, Inc.
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
public enum FrameType
{
    // blank type that is not defined
    UNDEFINED(0x00),
    // Connection
    SETUP(0x01, Flags.CAN_HAVE_DATA | Flags.CAN_HAVE_METADATA),
    SETUP_ERROR(0x02, 0),
    LEASE(0x03, 0),
    KEEPALIVE(0x04, 0),
    // Requester to start request
    REQUEST_RESPONSE(0x11, 0),
    FIRE_AND_FORGET(0x12, 0),
    REQUEST_STREAM(0x13, 0),
    REQUEST_SUBSCRIPTION(0x14, 0),
    REQUEST_CHANNEL(0x15, 0),
    // Requester mid-stream
    REQUEST_N(0x20, 0),
    CANCEL(0x21, 0),
    // Responder
    RESPONSE(0x30, 0),
    ERROR(0x31, 0),
    // Requester & Responder
    METADATA_PUSH(0x32, 0),
    // synthetic types from Responder for use by the rest of the machinery
    NEXT(0x33, 0),
    COMPLETE(0x34, 0),
    NEXT_COMPLETE(0x35, 0);

    private static class Flags
    {
        private static final int CAN_HAVE_DATA = 0b001;
        private static final int CAN_HAVE_METADATA = 0b010;
        private static final int IS_REQUEST_TYPE = 0b100;
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

    FrameType(final int id)
    {
        this(id, 0);
    }

    FrameType(int id, int flags) {
        this.id = id;
        this.flags = flags;
    }
    
    public int getEncodedType() {
        return id;
    }

    public boolean isRequestType()
    {
        return (Flags.IS_REQUEST_TYPE == (flags & Flags.IS_REQUEST_TYPE));
    }

    public boolean canHaveData()
    {
        return (Flags.CAN_HAVE_DATA == (flags & Flags.CAN_HAVE_DATA));
    }

    public boolean canHaveMetadata()
    {
        return (Flags.CAN_HAVE_METADATA == (flags & Flags.CAN_HAVE_METADATA));
    }

    // TODO: offset of metadata and data (simplify parsing) naming: endOfFrameHeaderOffset()
    public int payloadOffset()
    {
        return 0;
    }

    public static FrameType from(int id) {
        return typesById[id];
    }
}