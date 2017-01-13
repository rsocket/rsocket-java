package io.reactivesocket.server.leases;

import java.util.concurrent.TimeUnit;
import java.util.function.DoubleSupplier;

/**
 * PID controller implementation that is thread-safe. It uses {@code java.util.function.DoubleConsumer} functional
 * interfaces to supply the setpoint and kP, kI, and kD values. This allows the Pid controller to be dynamic.
 *
 */
public class DefaultPid implements Pid {
    private DoubleSupplier kP;
    private DoubleSupplier kI;
    private DoubleSupplier kD;
    private DoubleSupplier setpoint;

    private double errorSum;
    private double lastError;
    private double output;
    private long lastUpdateTs;

    public DefaultPid(DoubleSupplier setpoint, DoubleSupplier kP, DoubleSupplier kI, DoubleSupplier kD) {
        this.setpoint = setpoint;
        this.kP = kP;
        this.kI = kI;
        this.kD = kD;
        this.lastUpdateTs = System.currentTimeMillis();
    }

    public synchronized double update(double processValue) {
        long now = System.currentTimeMillis();
        long dt = TimeUnit.MILLISECONDS.toSeconds(now - lastUpdateTs);

        if (dt < 1) {
            dt = 1;
        }

        double error = setpoint.getAsDouble() - processValue;
        double p = kP.getAsDouble() * error;

        errorSum  = errorSum + error * dt;
        double i = kI.getAsDouble() * errorSum;

        double dError = (error - lastError) / dt;
        double d = kD.getAsDouble() * dError;

        output = p + limit(i) + limit(d);

        lastError = error;

        lastUpdateTs = now;

        return output;
    }

    public synchronized double getOutput() {
        return output;
    }

    double limit(double d) {
        if (d > 5.0) {
            return 5.0;
        } else if (d < -5.0) {
            return -5.0;
        }

        return d;
    }
}