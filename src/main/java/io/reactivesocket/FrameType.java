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
     SETUP(0x01, true),
     // Messages from Requestor (start a request)
     REQUEST_RESPONSE(0x11, true),
     FIRE_AND_FORGET(0x12, true),
     REQUEST_STREAM(0x13, true),
     REQUEST_SUBSCRIPTION(0x14, true),
     REQUEST_CHANNEL(0x15, true),
     // Messages from Requestor (mid-stream) 
     REQUEST_N(0x20, true),
     CANCEL(0x21, true),
     // Messages from Responder
     RESPONSE(0x30, false),
     ERROR(0x31, false),
     // synthetic types from Responder for use by the rest of the machinery
     NEXT(0x32, false),
     COMPLETE(0x33, false),
     NEXT_COMPLETE(0x34, false);

    private static FrameType[] typesById;

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

    private final int id;
    private final boolean requestType;

    FrameType(int id, boolean requestType) {
        this.id = id;
        this.requestType = requestType;
    }
    
    public int getEncodedType() {
        return id;
    }

    public boolean isRequestType()
    {
        return requestType;
    }

    public static FrameType from(int id) {
        return typesById[id];
    }
}