package io.reactivesocket.server.leases;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.DoubleConsumer;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;

/**
 * Calculates a setpoint by using the sign of the derivative of the different between outstanding and processed counters.
 * If the derivative is 0 or greater it will add one to the setpoint, otherwise it subtract one until it reeaches
 * zero again.
 */
public class DynamicSetpoint implements DoubleSupplier {
    private static final Logger logger = LoggerFactory.getLogger(DynamicSetpoint.class);
    private final LongSupplier outstanding;
    private final LongSupplier processed;
    private final DoubleConsumer onCalculateSetpoint;
    private final DoubleConsumer onCalculateDerivative;
    private double setpoint;
    private double lastDerivative;
    private long lastUpdateTs;
    private long lastOutstanding;
    private long lastProcessed;

    public DynamicSetpoint(LongSupplier outstanding, LongSupplier processed, DoubleConsumer onCalculateSetpoint, DoubleConsumer onCalculateDerivative) {
        this.outstanding = outstanding;
        this.processed = processed;
        this.lastUpdateTs = System.currentTimeMillis();
        this.onCalculateSetpoint = onCalculateSetpoint;
        this.onCalculateDerivative = onCalculateDerivative;
        this.setpoint = Runtime.getRuntime().availableProcessors() * 10;
    }

    public synchronized double calculateNewSetpoint() {
        long now = System.nanoTime();
        long dt = lastUpdateTs - now;
        long currentOutstanding = outstanding.getAsLong();
        long currentProcessed = processed.getAsLong();

        if (dt < 1) {
            dt = 1;
        }

        double diff = currentOutstanding - currentProcessed;
        double derivative = (diff - lastDerivative) / dt;

        if (onCalculateDerivative != null) {
            onCalculateDerivative.accept(derivative);
        }

        if (derivative > 0) {
            setpoint += 1;
        } else if (derivative < 0) {
            setpoint = Math.max(0, setpoint - 1);
        }

        if (onCalculateSetpoint != null) {
            onCalculateSetpoint.accept(setpoint);
        }

        lastUpdateTs = now;
        lastDerivative = derivative;
        lastOutstanding = currentOutstanding;
        lastProcessed = currentProcessed;

        logger.debug("dynamic setpoint calculated: setpoint => {}, diff => {}, derivative => {}", setpoint, diff, derivative);

        return setpoint;
    }

    @Override
    public synchronized double getAsDouble() {
        return setpoint;
    }

}
