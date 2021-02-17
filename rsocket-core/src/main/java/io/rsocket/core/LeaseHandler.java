package io.rsocket.core;

interface LeaseHandler {

  void handleLease();

  void handleError(Throwable t);
}
