package io.reactivesocket.aeron.internal;

public final class Constants {

    private Constants() {}

    public static final int SERVER_STREAM_ID = 1;

    public static final int CLIENT_STREAM_ID = 2;

    public static final byte[] EMTPY = new byte[0];

    public static final int QUEUE_SIZE = Integer.getInteger("framesSendQueueSize", 128);

    public static final int MULTI_THREADED_SPIN_LIMIT = Integer.getInteger("multiSpinLimit", 100);

}
