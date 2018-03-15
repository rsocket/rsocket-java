/*
 * Copyright 2015-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.aeron.internal;

import java.util.concurrent.TimeUnit;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.agrona.concurrent.SleepingIdleStrategy;

public final class Constants {

  public static final int SERVER_STREAM_ID = 0;
  public static final int CLIENT_STREAM_ID = 1;
  public static final int SERVER_MANAGEMENT_STREAM_ID = 10;
  public static final int CLIENT_MANAGEMENT_STREAM_ID = 11;
  public static final IdleStrategy EVENT_LOOP_IDLE_STRATEGY;
  public static final int AERON_MTU_SIZE = Integer.getInteger("aeron.mtu.length", 4096);

  static {
    String idlStrategy = System.getProperty("idleStrategy");

    if (NoOpIdleStrategy.class.getName().equalsIgnoreCase(idlStrategy)) {
      EVENT_LOOP_IDLE_STRATEGY = new NoOpIdleStrategy();
    } else if (SleepingIdleStrategy.class.getName().equalsIgnoreCase(idlStrategy)) {
      EVENT_LOOP_IDLE_STRATEGY = new SleepingIdleStrategy(TimeUnit.MILLISECONDS.toNanos(10));
    } else {
      EVENT_LOOP_IDLE_STRATEGY = new BackoffIdleStrategy(1, 10, 1_000, 100_000);
    }
  }

  private Constants() {}
}
