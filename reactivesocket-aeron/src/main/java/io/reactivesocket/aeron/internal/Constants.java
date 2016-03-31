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


import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.agrona.concurrent.SleepingIdleStrategy;

import java.util.concurrent.TimeUnit;

public final class Constants {

    public static final int SERVER_STREAM_ID = 1;
    public static final int CLIENT_STREAM_ID = 2;
    public static final byte[] EMTPY = new byte[0];
    public static final int QUEUE_SIZE = Integer.getInteger("reactivesocket.aeron.framesSendQueueSize", 262144);
    public static final IdleStrategy SERVER_IDLE_STRATEGY;
    public static final int AERON_MTU_SIZE = Integer.getInteger("aeron.mtu.length", 4096);
    public static final boolean TRACING_ENABLED = Boolean.getBoolean("reactivesocket.aeron.tracingEnabled");
    public static final int CLIENT_ESTABLISH_CONNECT_TIMEOUT_MS = 6000;
    public static final int CLIENT_SEND_ESTABLISH_CONNECTION_MSG_TIMEOUT_MS = 5000;
    public static final int SERVER_ACK_ESTABLISH_CONNECTION_TIMEOUT_MS = 3000;
    public static final int SERVER_ESTABLISH_CONNECTION_REQUEST_TIMEOUT_MS = 5000;
    public static final int SERVER_TIMER_WHEEL_TICK_DURATION_MS = 10;
    public static final int SERVER_TIMER_WHEEL_BUCKETS = 128;
    public static final int DEFAULT_OFFER_TO_AERON_TIMEOUT_MS = 30_000;

    static {
        String idlStrategy = System.getProperty("idleStrategy");

        if (NoOpIdleStrategy.class.getName().equalsIgnoreCase(idlStrategy)) {
            SERVER_IDLE_STRATEGY = new NoOpIdleStrategy();
        } else if (SleepingIdleStrategy.class.getName().equalsIgnoreCase(idlStrategy)) {
            SERVER_IDLE_STRATEGY = new SleepingIdleStrategy(TimeUnit.MILLISECONDS.toNanos(250));
        } else {
            SERVER_IDLE_STRATEGY = new BackoffIdleStrategy(1, 10, 100, 1000);
        }
    }

    private Constants() {
    }
}
