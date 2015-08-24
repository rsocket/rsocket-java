package io.reactivesocket.aeron;

import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import rx.Observable;
import rx.RxReactiveStreams;
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
    public Publisher<Void> addOutput(Publisher<Frame> o) {
        final Observable<Frame> frameObservable = RxReactiveStreams.toObservable(o);
        final Observable<Void> voidObservable = frameObservable
            .lift(new OperatorPublish(publication));

        return RxReactiveStreams.toPublisher(voidObservable);
    }

    @Override
    public void close() throws Exception {
        subscriber.onComplete();
        publication.close();
    }
}

