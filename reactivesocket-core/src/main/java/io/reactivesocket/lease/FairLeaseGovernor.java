/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.reactivesocket.lease;

import io.reactivesocket.Frame;
import io.reactivesocket.LeaseGovernor;
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
    private final int tickets;
    private final long period;
    private final TimeUnit unit;
    private final ScheduledExecutorService executor;

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

    public FairLeaseGovernor(int tickets, long period, TimeUnit unit, ScheduledExecutorService executor) {
        this.tickets = tickets;
        this.period = period;
        this.unit = unit;
        this.executor = executor;
        responders = new HashMap<>();
    }

    public FairLeaseGovernor(int tickets, long period, TimeUnit unit) {
        this(tickets, period, unit, Executors.newScheduledThreadPool(2, runnable -> {
            Thread thread = new Thread(runnable);
            thread.setDaemon(true);
            thread.setName("FairLeaseGovernor");
            return thread;
        }));
    }

    @Override
    public synchronized void register(Responder responder) {
        responders.put(responder, 0);
        if (runningTask == null) {
            final int ttl = (int)TimeUnit.NANOSECONDS.convert(period, unit);
            runningTask = executor.scheduleAtFixedRate(() -> distribute(ttl), 0, period, unit);
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
        Integer remainingTickets = responders.get(responder);
        if (remainingTickets != null) {
            remainingTickets--;
        } else {
            remainingTickets = -1;
        }
        responders.put(responder, remainingTickets);
        return remainingTickets >= 0;
    }
}
