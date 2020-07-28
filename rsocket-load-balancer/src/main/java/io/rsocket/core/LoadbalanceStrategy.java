package io.rsocket.core;

public interface LoadbalanceStrategy {

  PooledRSocket select(PooledRSocket[] availableRSockets);
}
