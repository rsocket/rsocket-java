/*
 * Copyright 2015-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.rsocket.lease;

import io.rsocket.exceptions.RejectedException;
import java.util.Objects;
import reactor.util.annotation.Nullable;

public class MissingLeaseException extends RejectedException {
  private static final long serialVersionUID = -6169748673403858959L;

  public MissingLeaseException(Lease lease, String tag) {
    super(leaseMessage(Objects.requireNonNull(lease), Objects.requireNonNull(tag)));
  }

  public MissingLeaseException(String tag) {
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
