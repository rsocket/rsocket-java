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
package io.reactivesocket.aeron.internal;

/**
 * Type of message being sent.
 */
public enum MessageType {
    ESTABLISH_CONNECTION_REQUEST(0x01),
    ESTABLISH_CONNECTION_RESPONSE(0x02),
    CONNECTION_DISCONNECT(0x3),
    FRAME(0x04);

    private static MessageType[] typesById;

    /**
     * Index types by id for indexed lookup.
     */
    static {
        int max = 0;

        for (MessageType t : values()) {
            max = Math.max(t.id, max);
        }

        typesById = new MessageType[max + 1];

        for (MessageType t : values()) {
            typesById[t.id] = t;
        }
    }

    private final int id;

    MessageType(int id) {
        this.id = id;
    }

    public int getEncodedType() {
        return id;
    }

    public static MessageType from(int id) {
        return typesById[id];
    }
}
