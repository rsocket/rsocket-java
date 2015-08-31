package io.reactivesocket.aeron;

import io.reactivesocket.Completable;
import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import uk.co.real_logic.aeron.Publication;

public class AeronClientDuplexConnection implements DuplexConnection, AutoCloseable {
    private  Publication publication;
    private Subscriber<? super Frame> subscriber;
    private Publisher<Frame> publisher;

    public AeronClientDuplexConnection(Publication publication) {
        this.publication = publication;
        this.publisher = (Subscriber<? super Frame> s) -> subscriber = s;
    }

    public Subscriber<? super Frame> getSubscriber() {
        return subscriber;
    }

    public Publisher<Frame> getInput() {
        return publisher;
    }

    @Override
    public void addOutput(Publisher<Frame> o, Completable callback) {
        o.subscribe(new CompletableSubscription(publication, callback));
    }

    @Override
    public void close() {
        subscriber.onComplete();
        publication.close();
    }
}

