package io.rsocket.lease;

import java.util.Set;
/** Allows granting leases on per-RSocket basis */
public class LeaseControl {
  private final LeaseRSocketRegistry leaseRSocketRegistry;

  LeaseControl(LeaseRSocketRegistry leaseRSocketRegistry) {
    this.leaseRSocketRegistry = leaseRSocketRegistry;
  }

  public Set<LeaseRSocketRef> getLeaseRSockets() {
    return leaseRSocketRegistry.getLeaseRSockets();
  }
}
