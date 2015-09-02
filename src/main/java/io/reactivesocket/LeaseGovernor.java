package io.reactivesocket;

import io.reactivesocket.internal.Responder;

public interface LeaseGovernor {
    public static final LeaseGovernor NULL_LEASE_GOVERNOR = new NullLeaseGovernor();
    public static final LeaseGovernor UNLIMITED_LEASE_GOVERNOR = new UnlimitedLeaseGovernor();

    void register(Responder responder);
    void unregister(Responder responder);
    void notify(Responder responder, Frame requestFrame);
}
