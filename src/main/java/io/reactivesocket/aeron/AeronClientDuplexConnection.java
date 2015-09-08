package io.reactivesocket.aeron;

import io.reactivesocket.Completable;
import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import rx.functions.Action0;
import uk.co.real_logic.aeron.Publication;

public class AeronClientDuplexConnection implements DuplexConnection, AutoCloseable {
    private  Publication publication;
    private Subscriber<? super Frame> subscriber;
    private Publisher<Frame> publisher;
    private Action0 handleClose;

    public AeronClientDuplexConnection(Publication publication, Action0 handleClose) {

        this.publication = publication;
        this.publisher = (Subscriber<? super Frame> s) -> {
            subscriber = s;
            s.onSubscribe(new Subscription() {
                @Override
                public void request(long n) {

                }

                @Override
                public void cancel() {

                }
            });
        };

        this.handleClose = handleClose;

    }

    public Subscriber<? super Frame> getSubscriber() {
        return subscriber;
    }

    public Publisher<Frame> getInput() {
        return publisher;
    }

    @Override
    public void addOutput(Publisher<Frame> o, Completable callback) {
        o.subscribe(new CompletableSubscription(publication, callback, this));
    }

    @Override
    public void close() {
        subscriber.onComplete();
        publication.close();
        handleClose.call();
    }
}

