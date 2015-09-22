package io.reactivesocket.aeron.client;

import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.aeron.AeronDuplexConnectionSubject;
import io.reactivesocket.aeron.internal.concurrent.AbstractConcurrentArrayQueue;
import io.reactivesocket.observable.Observable;
import io.reactivesocket.observable.Observer;
import uk.co.real_logic.aeron.Publication;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractClientDuplexConnection<T extends AbstractConcurrentArrayQueue<F>, F> implements DuplexConnection {
    protected final ArrayList<AeronDuplexConnectionSubject> subjects;

    protected final Publication publication;

    protected final T framesSendQueue;

    public AbstractClientDuplexConnection(Publication publication) {
        this.publication = publication;
        this.subjects = new ArrayList<>();
        this.framesSendQueue = createQueue();
    }

    @Override
    public final Observable<Frame> getInput() {
        AeronDuplexConnectionSubject subject = new AeronDuplexConnectionSubject(subjects);
        subjects.add(subject);
        return subject;
    }

    public final List<? extends Observer<Frame>> getSubscriber() {
        return subjects;
    }

    public final T getFramesSendQueue() {
        return framesSendQueue;
    }

    protected abstract T createQueue();
}
