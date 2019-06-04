package io.rsocket.exceptions;

import io.rsocket.lease.Lease;
import java.util.Objects;
import javax.annotation.Nonnull;

public class MissingLeaseException extends RejectedException {
  private static final long serialVersionUID = -6169748673403858959L;

  public MissingLeaseException(@Nonnull Lease lease, @Nonnull String tag) {
    super(leaseMessage(Objects.requireNonNull(lease), Objects.requireNonNull(tag)));
  }

  @Override
  public synchronized Throwable fillInStackTrace() {
    return this;
  }

  static String leaseMessage(Lease lease, String tag) {
    if (lease.isEmpty()) {
      return String.format("[%s] Lease was not received yet", tag);
    }
    boolean expired = lease.isExpired();
    int allowedRequests = lease.getAllowedRequests();
    return String.format(
        "[%s] Missing lease. Expired: %b, allowedRequests: %d", tag, expired, allowedRequests);
  }
}
