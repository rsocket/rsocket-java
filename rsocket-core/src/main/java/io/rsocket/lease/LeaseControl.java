package io.rsocket.lease;

import java.util.Set;
/** Allows granting leases on per-RSocket basis */
public class LeaseControl {
  private final LeaseRSocketRegistry leaseRSocketRegistry;

  public LeaseControl(LeaseRSocketRegistry leaseRSocketRegistry) {
    this.leaseRSocketRegistry = leaseRSocketRegistry;
  }

  public Set<LeaseRSocketRef> getLeaseRSockets() {
    return leaseRSocketRegistry.getLeaseRSockets();
  }
}
