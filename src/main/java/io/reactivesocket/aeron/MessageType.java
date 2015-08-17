package io.reactivesocket.aeron;

/**
 * Type of message being sent.
 */
enum MessageType {
    ESTABLISH_CONNECTION_REQUEST(0x01),
    ESTABLISH_CONNECTION_RESPONSE(0x02),
    FRAME(0x03);

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
