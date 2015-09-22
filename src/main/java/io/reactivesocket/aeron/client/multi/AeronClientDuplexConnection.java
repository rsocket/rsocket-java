package io.reactivesocket.aeron.client.multi;


import io.reactivesocket.Completable;
import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.aeron.AeronDuplexConnectionSubject;
import io.reactivesocket.aeron.internal.Constants;
import io.reactivesocket.aeron.internal.concurrent.ManyToManyConcurrentArrayQueue;
import io.reactivesocket.observable.Observable;
import io.reactivesocket.observable.Observer;
import org.reactivestreams.Publisher;
import rx.RxReactiveStreams;
import rx.exceptions.MissingBackpressureException;
import uk.co.real_logic.aeron.Publication;

import java.util.ArrayList;
import java.util.List;

public class AeronClientDuplexConnection implements DuplexConnection, AutoCloseable {
    private final Publication publication;
    private final ManyToManyConcurrentArrayQueue<FrameHolder> framesSendQueue;
    private final ArrayList<AeronDuplexConnectionSubject> subjects;

    public AeronClientDuplexConnection(Publication publication) {
        this.publication = publication;
        this.framesSendQueue = new ManyToManyConcurrentArrayQueue<>(Constants.CONCURRENCY);
        this.subjects = new ArrayList<>();
    }

    public List<? extends Observer<Frame>> getSubscriber() {
        return subjects;
    }

    @Override
    public Observable<Frame> getInput() {
        AeronDuplexConnectionSubject subject = new AeronDuplexConnectionSubject(subjects);
        subjects.add(subject);
        return subject;
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
                        if (!offer && ++i > 100) {
                            rx.Observable.error(new MissingBackpressureException());
                        }
                    } while (!offer);
                });
            }, Constants.CONCURRENCY)
            .subscribe(ignore -> {
            }, callback::error, callback::success);
    }

    public ManyToManyConcurrentArrayQueue<FrameHolder> getFramesSendQueue() {
        return framesSendQueue;
    }

    @Override
    public void close() {
    }
}
