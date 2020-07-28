package io.rsocket.core;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public class RoundRobinLoadbalanceStrategy implements LoadbalanceStrategy {

  volatile int nextIndex;

  static final AtomicIntegerFieldUpdater<RoundRobinLoadbalanceStrategy> NEXT_INDEX =
      AtomicIntegerFieldUpdater.newUpdater(RoundRobinLoadbalanceStrategy.class, "nextIndex");

  @Override
  public PooledRSocket select(PooledRSocket[] sockets) {
    int length = sockets.length;

    int indexToUse = Math.abs(NEXT_INDEX.getAndIncrement(this) % length);

    final PooledRSocket pooledRSocket = sockets[indexToUse];
    System.out.println(pooledRSocket.stats());
    return pooledRSocket;
  }
}
