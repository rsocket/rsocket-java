package io.rsocket.stat;

import io.rsocket.DuplexConnection;
import io.rsocket.Frame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.SignalType;

/**
 *
 */
public class StatsPrinter implements RSocketStats {

    private Logger logger;

    public StatsPrinter() {
        logger = LoggerFactory.getLogger(RSocketStats.class);
    }

    @Override
    public void socketCreated() {
        logger.info("socketCreated");
    }

    @Override
    public void socketDisconnected() {
        logger.info("socketDisconnected");
    }

    @Override
    public void socketClosed(SignalType signalType) {
        logger.info("socketClosed");
    }

    @Override
    public void duplexConnectionCreated(DuplexConnection connection) {
        logger.info("connectionCreated " + connection.getClass().getName());
    }

    @Override
    public void duplexConnectionClosed(DuplexConnection connection) {
        logger.info("connectionClosed " + connection.getClass().getName());
    }

    @Override
    public void frameWritten(Frame frame) {
        logger.info("frameWritten " + frame.getType());
    }

    @Override
    public void frameRead(Frame frame) {
        logger.info("frameRead " + frame.getType());
    }

}
