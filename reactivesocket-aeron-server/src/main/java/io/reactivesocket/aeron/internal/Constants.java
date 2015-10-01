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

import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.agrona.concurrent.NoOpIdleStrategy;

public final class Constants {

    private Constants() {}

    public static final int SERVER_STREAM_ID = 1;

    public static final int CLIENT_STREAM_ID = 2;

    public static final byte[] EMTPY = new byte[0];

    public static final int QUEUE_SIZE = Integer.getInteger("reactivesocket.aeron.framesSendQueueSize", 262144);

    public static final IdleStrategy SERVER_IDLE_STRATEGY = new NoOpIdleStrategy();

    public static final int CONCURRENCY = Integer.getInteger("reactivesocket.aeron.clientConcurrency", 2);

    public static final int AERON_MTU_SIZE = Integer.getInteger("aeron.mtu.length", 4096);

    public static final boolean TRACING_ENABLED = Boolean.getBoolean("reactivesocket.aeron.tracingEnabled");

}
