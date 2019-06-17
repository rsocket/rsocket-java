package io.rsocket.exceptions;

import io.rsocket.lease.Lease;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class MissingLeaseException extends RejectedException {
  private static final long serialVersionUID = -6169748673403858959L;

  public MissingLeaseException(@Nonnull Lease lease, @Nonnull String tag) {
    super(leaseMessage(Objects.requireNonNull(lease), Objects.requireNonNull(tag)));
  }

  public MissingLeaseException(@Nonnull String tag) {
    super(leaseMessage(null, Objects.requireNonNull(tag)));
  }

  @Override
  public synchronized Throwable fillInStackTrace() {
    return this;
  }

  static String leaseMessage(@Nullable Lease lease, String tag) {
    if (lease == null) {
      return String.format("[%s] Missing leases", tag);
    }
    if (lease.isEmpty()) {
      return String.format("[%s] Lease was not received yet", tag);
    }
    boolean expired = lease.isExpired();
    int allowedRequests = lease.getAllowedRequests();
    return String.format(
        "[%s] Missing leases. Expired: %b, allowedRequests: %d", tag, expired, allowedRequests);
  }
}
