package io.reactivesocket.server.leases;


import io.reactivesocket.stat.Ewma;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.function.DoubleConsumer;
import java.util.function.DoubleSupplier;
import java.util.function.IntConsumer;
import java.util.function.IntSupplier;

/**
 * Calculates leases using two pid controllers. One controller is used to calculate the number of leases needed to have
 * zero {@link io.reactivesocket.exceptions.RejectedException}s. The other pid controller is used to control concurrency.
 * The do not run at the same time. The rejection based pid controller runs when there are no pending requests. If there
 * are pending requests the concurrency controller is used instead.
 * <p>
 * The control will also decay the number of leases if the current leases are 1.5 times greater than the estimated leases.
 *
 * @author Robert Roeser
 */
public class PidCapacitySupplier implements IntSupplier {
    private static final Logger logger = LoggerFactory.getLogger(PidCapacitySupplier.class);

    private final ReactiveSocketServerStats stats;

    private final Pid rejectionPid;

    private final Pid concurrencyPid;

    private final DynamicSetpoint setpoint;

    private double leases;

    private double lastRejected;

    private double lastProcessed;

    private final IntConsumer onCalculateLease;

    private final IntSupplier overrideLeases;

    private final Ewma estimatedLeasesAvg;

    public PidCapacitySupplier(
        ReactiveSocketServerStats stats,
        IntSupplier overrideLeases,
        DoubleSupplier kP,
        DoubleSupplier kI,
        DoubleSupplier kD,
        DoubleConsumer onCalculateSetpoint,
        DoubleConsumer onCalculateDerivative,
        IntConsumer onCalculateLease,
        long ttlMillis) {
        this(stats, overrideLeases, onCalculateSetpoint, onCalculateDerivative, onCalculateLease, kP, kI, kD, kP, kI, kD, ttlMillis);
    }

    public PidCapacitySupplier(ReactiveSocketServerStats stats,
                                IntSupplier overrideLeases,
                                DoubleConsumer onCalculateSetpoint,
                                DoubleConsumer onCalculateDerivative,
                                IntConsumer onCalculateLease,
                                DoubleSupplier rejections_kP,
                                DoubleSupplier rejections_kI,
                                DoubleSupplier rejections_kD,
                                DoubleSupplier concurrency_kP,
                                DoubleSupplier concurrency_kI,
                                DoubleSupplier concurrency_kD,
                                long ttlMillis) {
        this.onCalculateLease = onCalculateLease;
        this.overrideLeases = overrideLeases;
        this.stats = stats;
        this.setpoint = new DynamicSetpoint(
            stats::getCurrentOustanding,
            stats::getCurrentProcessed,
            onCalculateSetpoint,
            onCalculateDerivative);

        this.rejectionPid = new DefaultPid(() -> 0, rejections_kP, rejections_kI, rejections_kD);
        this.concurrencyPid = new DefaultPid(setpoint, concurrency_kP, concurrency_kI, concurrency_kD);

        this.leases = 1;
        estimatedLeasesAvg = new Ewma(ttlMillis / 2, TimeUnit.MINUTES.toMillis(5), TimeUnit.MILLISECONDS, 0.0);
    }

    @Override
    public synchronized int getAsInt() {
        final double processed = stats.getCurrentProcessed();
        final double rejected = stats.getCurrentRejected();

        double currentEstimate = (processed - lastProcessed) * 1.5;
        estimatedLeasesAvg.insert(currentEstimate);
        double estimatedLeases = estimatedLeasesAvg.value();

        logger.debug("current estimated leases => {}", estimatedLeases);

        if (overrideLeases != null && overrideLeases.getAsInt() > 0) {
            logger.debug("overriding leases => " + overrideLeases.getAsInt());
            return overrideLeases.getAsInt();
        }

        boolean useConcurrency = calculateLeasesBasedOnConcurrency(processed);
        if (!useConcurrency) {
            logger.debug("zero pending requests, basing leases on rejects");
            calculateLeasesBasedOnRejections(rejected, estimatedLeases);
        }

        lastProcessed = processed;
        lastRejected = rejected;

        int l = (int) Math.ceil(leases);
        if (onCalculateLease != null) {
            onCalculateLease.accept(l);
        }

        return l;
    }

    private boolean calculateLeasesBasedOnConcurrency(double processed) {
        final double outstanding = stats.getCurrentOustanding();
        final double pending = outstanding - processed;

        final double currentSetpoint = setpoint.getAsDouble();
        final double newSetpoint = setpoint.calculateNewSetpoint();

        logger.debug("outstanding => {}, " +
            "processed = > {}, " +
            "pending => {}, " +
            "current setpoint => {}, " +
            "new setpoint => {}", outstanding, processed, pending, currentSetpoint, newSetpoint);

        boolean useConcurrency = false;
        if (pending > 0) {
            useConcurrency = true;
            double update = concurrencyPid.update(pending);
            double oldLeases = leases;
            leases += update;

            logger.debug("using concurrency => true," +
                " update => {}," +
                " old leases => {}," +
                " new leases => {}", update, oldLeases, leases);
        }

        return useConcurrency;
    }

    private void calculateLeasesBasedOnRejections(double rejected, double estimatedLeases) {
        double processValue = lastRejected - rejected;
        double update = rejectionPid.update(processValue);
        double oldLeases = leases;

        if (processValue < 0) {
            logger.debug("process value was zero, not updating leases");
            leases += update;
        } else {
            decay(estimatedLeases);
        }

        if (leases < 0) {
            logger.debug("calculated leases less than zero => {}, setting to 1", leases);
            leases = 1;
        }

        logger.debug("rejection pid: " +
            "\nrejected => {}," +
            "\nprocessValue => {}," +
            "\nupdate => {}," +
            "\nold leases => {}," +
            "\nnew leases => {}", rejected, processValue, update, oldLeases, leases);

    }

    private void decay(double estimatedLeases) {
        if (estimatedLeases > 0 && leases > estimatedLeases) {
            double oldLeases = leases;
            double diff = leases - estimatedLeases;
            leases -= diff;
            logger.debug("decaying - old leases => {}, diff => {}, leases => {}", oldLeases, diff, leases);
        }
    }
}