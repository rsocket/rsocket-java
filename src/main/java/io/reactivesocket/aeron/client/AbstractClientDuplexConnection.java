package io.reactivesocket.aeron.client;

import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.rx.Disposable;
import io.reactivesocket.rx.Observable;
import io.reactivesocket.rx.Observer;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.agrona.concurrent.AbstractConcurrentArrayQueue;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractClientDuplexConnection<T extends AbstractConcurrentArrayQueue<F>, F> implements DuplexConnection {
    protected final static AtomicInteger count = new AtomicInteger();

    protected final int connectionId;

    protected final CopyOnWriteArrayList<Observer<Frame>> subjects;

    protected final T framesSendQueue;

    public AbstractClientDuplexConnection(Publication publication) {
        this.subjects = new CopyOnWriteArrayList<>();
        this.framesSendQueue = createQueue();
        this.connectionId = count.incrementAndGet();
    }

    @Override
    public final Observable<Frame> getInput() {
        return new Observable<Frame>() {
            public void subscribe(Observer<Frame> o) {
                o.onSubscribe(new Disposable() {
                    @Override
                    public void dispose() {
                        subjects.removeIf(s -> s == o);
                    }
                });
                subjects.add(o);
            }
        };
    }

    public final List<? extends Observer<Frame>> getSubscriber() {
        return subjects;
    }

    public final T getFramesSendQueue() {
        return framesSendQueue;
    }

    protected abstract T createQueue();

    public int getConnectionId() {
        return connectionId;
    }
}
