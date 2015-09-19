package io.reactivesocket.aeron.client.multi;


import io.reactivesocket.Completable;
import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.aeron.internal.Constants;
import io.reactivesocket.aeron.internal.concurrent.ManyToManyConcurrentArrayQueue;
import io.reactivesocket.internal.EmptyDisposable;
import io.reactivesocket.observable.Observable;
import io.reactivesocket.observable.Observer;
import org.reactivestreams.Publisher;
import rx.RxReactiveStreams;
import rx.Scheduler;
import rx.exceptions.MissingBackpressureException;
import rx.schedulers.Schedulers;
import uk.co.real_logic.aeron.Publication;

public class AeronClientDuplexConnection implements DuplexConnection, AutoCloseable {
    private Publication publication;
    private Observer<Frame> observer;
    private Observable<Frame> observable;
    private ManyToManyConcurrentArrayQueue<FrameHolder> framesSendQueue;
    private Scheduler.Worker worker;

    public AeronClientDuplexConnection(Publication publication) {
        this.publication = publication;
        this.framesSendQueue = new ManyToManyConcurrentArrayQueue<>(Constants.CONCURRENCY);
        this.observable = (Observer<Frame> o) -> {
            observer = o;
            observer.onSubscribe(new EmptyDisposable());
        };
        this.worker = Schedulers.computation().createWorker();

    }

    public Observer<Frame> getSubscriber() {
        return observer;
    }

    @Override
    public Observable<Frame> getInput() {
        return observable;
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
        worker.unsubscribe();
    }
}
