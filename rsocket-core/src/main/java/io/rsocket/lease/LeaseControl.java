package io.rsocket.lease;

import java.util.Set;

public class LeaseControl {
  private final LeaseRSocketRegistry leaseRSocketRegistry;

  public LeaseControl(LeaseRSocketRegistry leaseRSocketRegistry) {
    this.leaseRSocketRegistry = leaseRSocketRegistry;
  }

  public Set<LeaseRSocketRef> getLeaseRSockets() {
    return leaseRSocketRegistry.getLeaseRSockets();
  }
}
