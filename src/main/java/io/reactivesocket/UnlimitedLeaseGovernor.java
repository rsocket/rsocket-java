package io.reactivesocket;

import io.reactivesocket.internal.Responder;

public class UnlimitedLeaseGovernor implements LeaseGovernor {
    @Override
    public void register(Responder responder) {
        responder.sendLease(Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    @Override
    public void unregister(Responder responder) {}

    @Override
    public void notify(Responder responder, Frame requestFrame) {}
}
