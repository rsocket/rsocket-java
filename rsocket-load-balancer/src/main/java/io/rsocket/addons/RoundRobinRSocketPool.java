package io.rsocket.addons;

import io.rsocket.RSocket;
import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.reactivestreams.Publisher;

public class RoundRobinRSocketPool extends BaseRSocketPool {

  volatile int nextIndex;

  static final AtomicIntegerFieldUpdater<RoundRobinRSocketPool> NEXT_INDEX =
      AtomicIntegerFieldUpdater.newUpdater(RoundRobinRSocketPool.class, "nextIndex");

  public RoundRobinRSocketPool(Publisher<List<RSocket>> source) {
    super(source);
  }

  @Override
  RSocket doSelect() {
    RSocket[] sockets = this.activeSockets;
    int length = sockets.length;
    if (sockets == EMPTY) {
      return null;
    }

    int indexToUse = Math.abs(NEXT_INDEX.getAndIncrement(this) % length);

    return sockets[indexToUse];
  }
}
