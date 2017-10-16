package io.rsocket.lease;

class LeaseGranterTestUtils {
  static Lease newLease(int numberOfRequests, int ttl) {
    return new LeaseImpl(numberOfRequests, ttl, null);
  }
}
