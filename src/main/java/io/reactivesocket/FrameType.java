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
     SETUP(0x01),
     // Messages from Requestor
     REQUEST_RESPONSE(0x11),
     FIRE_AND_FORGET(0x12),
     REQUEST_STREAM(0x13),
     REQUEST_SUBSCRIPTION(0x14),
     REQUEST_N(0x15),
     CANCEL(0x16),
     // Messages from Responder
     NEXT(0x22),
     COMPLETE(0x23),
     ERROR(0x24);
    
    private static FrameType[] typesById;

    /**
     * Index types by id for indexed lookup. 
     */
    static {
        int max = 0;
        for (FrameType t : values()) {
            if (t.id > max) {
                max = t.id;
            }
        }
        typesById = new FrameType[max + 1];
        for (FrameType t : values()) {
            typesById[t.id] = t;
        }
    }

    private final int id;

    FrameType(int id) {
        this.id = id;
    }
    
    public int getMessageId() {
        return id;
    }

    public static FrameType from(int id) {
        return typesById[id];
    }
}