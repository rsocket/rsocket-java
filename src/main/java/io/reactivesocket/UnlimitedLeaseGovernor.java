package io.reactivesocket;

import io.reactivesocket.internal.Responder;

public class UnlimitedLeaseGovernor implements LeaseGovernor {
    @Override
    public void register(Responder responder) {
        responder.sendLease(Long.MAX_VALUE, Long.MAX_VALUE);
    }

    @Override
    public void unregister(Responder responder) {}

    @Override
    public void notify(Responder responder, Frame requestFrame) {}
}
