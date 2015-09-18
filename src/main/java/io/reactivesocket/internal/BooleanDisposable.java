package io.reactivesocket.internal;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import io.reactivesocket.observable.Disposable;

public final class BooleanDisposable implements Disposable {
    volatile Runnable run;

    static final AtomicReferenceFieldUpdater<BooleanDisposable, Runnable> RUN =
            AtomicReferenceFieldUpdater.newUpdater(BooleanDisposable.class, Runnable.class, "run");
    
    static final Runnable DISPOSED = () -> { };

    public BooleanDisposable() {
        this(() -> { });
    }
    
    public BooleanDisposable(Runnable run) {
        RUN.lazySet(this, run);
    }
    
    @Override
    public void dispose() {
        Runnable r = run;
        if (r != DISPOSED) {
            r = RUN.getAndSet(this, DISPOSED);
            if (r != DISPOSED) {
                r.run();
            }
        }
    }
    
    public boolean isDisposed() {
        return run == DISPOSED;
    }
}