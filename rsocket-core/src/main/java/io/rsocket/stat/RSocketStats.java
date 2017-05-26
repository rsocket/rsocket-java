package io.rsocket.stat;

import io.rsocket.DuplexConnection;
import io.rsocket.Frame;
import io.rsocket.FrameType;
import reactor.core.publisher.SignalType;

/**
 *
 */
public interface RSocketStats {

    // TODO - I could not well figure out when to call socket Create/Disconnect/Closed functions
    void socketCreated();
    void socketDisconnected();
    void socketClosed(SignalType signalType);

    void duplexConnectionCreated(DuplexConnection connection);
    void duplexConnectionClosed(DuplexConnection connection);

    void frameWritten(Frame frame);
    void frameRead(Frame frame);
}
