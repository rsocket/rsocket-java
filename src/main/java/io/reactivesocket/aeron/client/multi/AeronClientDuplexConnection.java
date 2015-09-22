package io.reactivesocket.aeron.client.multi;


import io.reactivesocket.Completable;
import io.reactivesocket.Frame;
import io.reactivesocket.aeron.client.AbstractClientDuplexConnection;
import io.reactivesocket.aeron.internal.Constants;
import io.reactivesocket.aeron.internal.concurrent.ManyToManyConcurrentArrayQueue;
import org.reactivestreams.Publisher;
import rx.RxReactiveStreams;
import rx.exceptions.MissingBackpressureException;
import uk.co.real_logic.aeron.Publication;

public class AeronClientDuplexConnection extends AbstractClientDuplexConnection<ManyToManyConcurrentArrayQueue<FrameHolder>, FrameHolder> {
    public AeronClientDuplexConnection(Publication publication) {
        super(publication);
    }

    @Override
    protected ManyToManyConcurrentArrayQueue<FrameHolder> createQueue() {
        return new ManyToManyConcurrentArrayQueue<>(Constants.CONCURRENCY);
    }

    @Override
    public void addOutput(Publisher<Frame> o, Completable callback) {
        rx.Observable<Frame> frameObservable = RxReactiveStreams.toObservable(o);
        frameObservable
            .flatMap(frame -> {
                return rx.Observable.<FrameHolder>create(subscriber -> {
                    final FrameHolder frameHolder = FrameHolder.get(frame, publication, subscriber);
                    subscriber.onNext(frameHolder);
                })
                .doOnNext(fh -> {
                    boolean offer = false;
                    int i = 0;
                    do {
                        offer = framesSendQueue.offer(fh);
                        if (!offer && ++i > Constants.MULTI_THREADED_SPIN_LIMIT) {
                            rx.Observable.error(new MissingBackpressureException());
                        }
                    } while (!offer);
                });
            }, Constants.CONCURRENCY)
            .subscribe(ignore -> {
            }, callback::error, callback::success);
    }

    @Override
    public void close() {
    }

}
