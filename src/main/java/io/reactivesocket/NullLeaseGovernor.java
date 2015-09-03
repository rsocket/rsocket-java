package io.reactivesocket;

import io.reactivesocket.internal.Responder;

public class NullLeaseGovernor implements LeaseGovernor {
    @Override
    public void register(Responder responder) {}

    @Override
    public void unregister(Responder responder) {}

    @Override
    public void notify(Responder responder, Frame requestFrame) {}
}
