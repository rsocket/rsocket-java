package io.rsocket.stat;

import io.rsocket.DuplexConnection;
import io.rsocket.Frame;
import reactor.core.publisher.SignalType;

/**
 *
 */
public class BaseStats implements RSocketStats{

    @Override
    public void socketCreated() {}

    @Override
    public void socketDisconnected() {}

    @Override
    public void socketClosed(SignalType signalType) {}

    @Override
    public void duplexConnectionCreated(DuplexConnection connection) {}

    @Override
    public void duplexConnectionClosed(DuplexConnection connection) {}

    @Override
    public void frameWritten(Frame frame) {}

    @Override
    public void frameRead(Frame frame) {}

}
