package io.rsocket.stat;

import io.rsocket.DuplexConnection;
import io.rsocket.Frame;
import reactor.core.publisher.SignalType;

/**
 *
 */
public interface RSocketStats {
    void socketCreated();
    void socketDisconnected();
    void socketClosed(SignalType signalType);

    void duplexConnectionCreated(DuplexConnection connection);
    void duplexConnectionClosed(DuplexConnection connection);

    void frameWritten(Frame frame);
    void frameRead(Frame frame);
}
