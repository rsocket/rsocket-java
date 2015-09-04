package io.reactivesocket;

import io.reactivesocket.internal.Responder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Distribute evenly a static number of tickets to all connected clients.
 */
public class FairLeaseGovernor implements LeaseGovernor {
    private static ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(1);

    private final int tickets;
    private final long period;
    private final TimeUnit unit;
    private final Map<Responder, Integer> responders;
    private ScheduledFuture<?> runningTask;

    private synchronized void distribute(int ttlMs) {
        if (!responders.isEmpty()) {
            int budget = tickets / responders.size();

            // it would be more fair to randomized the distribution of extra
            int extra = tickets - budget * responders.size();
            for (Responder responder: responders.keySet()) {
                int n = budget;
                if (extra > 0) {
                    n += 1;
                    extra -= 1;
                }
                responder.sendLease(ttlMs, n);
                responders.put(responder, n);
            }
        }
    }

    public FairLeaseGovernor(int tickets, long period, TimeUnit unit) {
        this.tickets = tickets;
        this.period = period;
        this.unit = unit;
        responders = new HashMap<>();
    }

    @Override
    public synchronized void register(Responder responder) {
        responders.put(responder, 0);
        if (runningTask == null) {
            final int ttl = (int)TimeUnit.NANOSECONDS.convert(period, unit);
            runningTask = EXECUTOR.scheduleAtFixedRate(() -> distribute(ttl), 0, period, unit);
        }
    }

    @Override
    public synchronized void unregister(Responder responder) {
        responders.remove(responder);
        if (responders.isEmpty() && runningTask != null) {
            runningTask.cancel(true);
            runningTask = null;
        }
    }

    @Override
    public synchronized boolean accept(Responder responder, Frame frame) {
        boolean valid = false;
        final Integer remainingTickets = responders.get(responder);
        return remainingTickets == null || remainingTickets > 0;
    }
}
