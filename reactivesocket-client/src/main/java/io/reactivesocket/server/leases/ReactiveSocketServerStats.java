package io.reactivesocket.server.leases;

import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class ReactiveSocketServerStats {
    private AtomicLong outstanding;
    private AtomicLong processed;
    private AtomicLong successful;
    private AtomicLong rejected;

    public ReactiveSocketServerStats() {
        outstanding = new AtomicLong();
        processed = new AtomicLong();
        successful = new AtomicLong();
        rejected = new AtomicLong();
    }

    public void incrementOutstanding() {
        outstanding.incrementAndGet();
    }

    public void incrementProcessed() {
        processed.incrementAndGet();
    }

    public void incrementSuccessful() {
        successful.incrementAndGet();
    }

    public void incrementRejected() {
        rejected.incrementAndGet();
    }

    public long getCurrentOustanding() {
        return outstanding.get();
    }

    public long getCurrentProcessed() {
        return processed.get();
    }

    public long getCurrentSuccessful() {
        return successful.get();
    }

    public long getCurrentRejected() {
        return rejected.get();
    }
}
