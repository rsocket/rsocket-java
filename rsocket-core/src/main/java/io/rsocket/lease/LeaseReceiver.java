package io.rsocket.lease;

public interface LeaseReceiver {

  void receiveLease(Lease lease);

  Lease requesterLease();
}
