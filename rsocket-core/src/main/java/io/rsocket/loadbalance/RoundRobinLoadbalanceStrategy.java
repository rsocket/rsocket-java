/*
 * Copyright 2015-2021 the original author or authors.
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
package io.rsocket.loadbalance;

import io.rsocket.RSocket;
import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * Simple {@link LoadbalanceStrategy} that selects the {@code RSocket} to use in round-robin order.
 *
 * @since 1.1
 */
public class RoundRobinLoadbalanceStrategy implements LoadbalanceStrategy {

  volatile int nextIndex;

  private static final AtomicIntegerFieldUpdater<RoundRobinLoadbalanceStrategy> NEXT_INDEX =
      AtomicIntegerFieldUpdater.newUpdater(RoundRobinLoadbalanceStrategy.class, "nextIndex");

  @Override
  public RSocket select(List<RSocket> sockets) {
    int length = sockets.size();

    int indexToUse = Math.abs(NEXT_INDEX.getAndIncrement(this) % length);

    return sockets.get(indexToUse);
  }
}
