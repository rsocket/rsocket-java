/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.reactivesocket.aeron.server;

import io.reactivesocket.Frame;
import io.reactivesocket.LeaseGovernor;
import io.reactivesocket.internal.Responder;
import org.agrona.TimerWheel;
import org.agrona.collections.Int2IntHashMap;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Lease Governor that evenly distributes requests all connected clients. The work is done using the
 * {@link ServerAeronManager}'s {@link TimerWheel}
 */
public class TimerWheelFairLeaseGovernor implements LeaseGovernor, Runnable {
    private final int tickets;
    private final long period;
    private final int ttlMs;
    private final TimeUnit unit;
    private final TimerWheel.Timer timer;
    private final List<Responder> responders;
    private final Int2IntHashMap leaseCount;

    private boolean running = false;

    private int ticketsPerResponder = 0;

    private int extra = 0;

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
        if (running) {
            try {
                final int numResponders = responders.size();
                if (numResponders > 0) {
                    int extraTicketsLeft = extra;

                    for (int i = 0; i < numResponders; i++) {
                        int amountToSend = ticketsPerResponder;
                        if (extraTicketsLeft > 0) {
                            amountToSend++;
                            extraTicketsLeft--;
                        }
                        Responder responder = responders.get(i);
                        leaseCount.put(responder.hashCode(), amountToSend);
                        responder.sendLease(ttlMs, amountToSend);
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
        ServerAeronManager.getInstance().submitAction(() -> {
            responders.add(responder);

            calculateTicketsToSendPerResponder();

            if (!running) {
                running = true;
                run();
            }
        });
    }

    @Override
    public void unregister(Responder responder) {
        ServerAeronManager.getInstance().submitAction(() -> {
            responders.remove(responder);

            calculateTicketsToSendPerResponder();

            if (running && responders.isEmpty()) {
                running = false;
            }
        });
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
        int count = leaseCount.get(responder.hashCode()) - 1;

        if (count >= 0) {
            leaseCount.put(responder.hashCode(), count);
        }

        return count > 0;

    }
}