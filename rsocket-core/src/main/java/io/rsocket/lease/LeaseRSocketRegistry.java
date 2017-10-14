package io.rsocket.lease;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

public class LeaseRSocketRegistry {
  private final Set<LeaseRSocketRef> leaseConns = new ConcurrentSkipListSet<>();

  public void addLeaseRSocket(LeaseRSocketRef leaseRSocketRef) {
    leaseConns.add(leaseRSocketRef);
    leaseRSocketRef.onClose().doOnTerminate(() -> leaseConns.remove(leaseRSocketRef)).subscribe();
  }

  public Set<LeaseRSocketRef> getLeaseRSockets() {
    return new HashSet<>(leaseConns);
  }
}
