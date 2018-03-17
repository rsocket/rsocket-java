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

package io.rsocket.util;

import java.util.concurrent.TimeUnit;

/** Abstraction to get current time and durations. */
public final class Clock {

  private Clock() {
    // No Instances.
  }

  public static long now() {
    return System.nanoTime() / 1000;
  }

  public static long elapsedSince(long timestamp) {
    long t = now();
    return Math.max(0L, t - timestamp);
  }

  public static TimeUnit unit() {
    return TimeUnit.MICROSECONDS;
  }
}
