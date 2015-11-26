package io.reactivesocket.aeron.server;

import io.reactivesocket.Frame;
import io.reactivesocket.LeaseGovernor;
import io.reactivesocket.internal.Responder;
import uk.co.real_logic.agrona.TimerWheel;
import uk.co.real_logic.agrona.collections.Int2IntHashMap;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Lease Governor that evenly distributes requests all connected clients. The work is done using the
 * {@link ServerAeronManager}'s {@link uk.co.real_logic.agrona.TimerWheel}
 */
public class TimerWheelFairLeaseGovernor implements LeaseGovernor, Runnable {
    private final int tickets;
    private final long period;
    private final int ttlMs;
    private final TimeUnit unit;
    private final TimerWheel.Timer timer;
    private final List<Responder> responders;
    private final Int2IntHashMap leaseCount;

    private volatile boolean running = false;
    private long p0, p1, p2, p3, p4, p5, p6;

    private volatile int ticketsPerResponder = 0;
    private long p10, p11, p12, p13, p14, p15, p16;

    private volatile int extra = 0;
    private long p20, p21, p22, p23, p24, p25, p26;

    public TimerWheelFairLeaseGovernor(int tickets, long period, TimeUnit unit) {
        this.responders = new ArrayList<>();
        this.leaseCount = new Int2IntHashMap(0);
        this.tickets = tickets;
        this.period = period;
        this.unit = unit;
        this.ttlMs = (int) unit.toMillis(period);
        this.timer = ServerAeronManager
                .getInstance()
                .getTimerWheel()
                .newBlankTimer();
    }

    @Override
    public void run() {
        if (!running) {
            try {
                synchronized (responders) {
                    final int numResponders = responders.size();
                    if (numResponders > 0) {
                        final int extraWinner = ((int) System.nanoTime()) % numResponders;

                        for (int i = 0; i < numResponders; i++) {
                            int amountToSend = ticketsPerResponder;
                            if (i == extraWinner) {
                                amountToSend += extra;
                            }
                            Responder responder = responders.get(i);
                            leaseCount.put(responder.hashCode(), amountToSend);
                            responder.sendLease(ttlMs, amountToSend);
                        }
                    }
                }
            } finally {
                ServerAeronManager
                        .getInstance()
                        .getTimerWheel()
                        .rescheduleTimeout(period, unit, timer, this::run);
            }
        }
    }

    @Override
    public void register(Responder responder) {
        synchronized (responders) {
            responders.add(responder);

            calculateTicketsToSendPerResponder();

            if (!running) {
                running = false;
                run();
            }
        }
    }

    @Override
    public void unregister(Responder responder) {
        synchronized (responders) {
            responders.remove(responder);

            calculateTicketsToSendPerResponder();

            if (running && responders.isEmpty()) {
                running = false;
            }
        }
    }

    void calculateTicketsToSendPerResponder() {
        int size = this.responders.size();
        if (size > 0) {
            ticketsPerResponder = tickets / size;
            extra = tickets - ticketsPerResponder * size;
        }
    }

    @Override
    public boolean accept(Responder responder, Frame frame) {
        int count;
        synchronized (responders) {
            count = leaseCount.get(responder.hashCode()) - 1;

            if (count >= 0) {
                leaseCount.put(responder.hashCode(), count);
            }
        }

        return count > 0;

    }
}