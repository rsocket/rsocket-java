package io.rsocket.exceptions;

import io.rsocket.lease.Lease;
import javax.annotation.Nonnull;

public class NoLeaseException extends RejectedException {

  public NoLeaseException(@Nonnull Lease lease) {
    super(leaseMessage(lease));
  }

  private static String leaseMessage(Lease lease) {
    boolean expired = lease.isExpired();
    int allowedRequests = lease.getAllowedRequests();
    return String.format(
        "Missing lease. Expired: %b, allowedRequests: %d", expired, allowedRequests);
  }
}
