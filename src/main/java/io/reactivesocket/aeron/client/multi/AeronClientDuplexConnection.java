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
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import rx.Scheduler;
import rx.schedulers.Schedulers;
import uk.co.real_logic.aeron.Publication;

public class AeronClientDuplexConnection implements DuplexConnection, AutoCloseable {
    private Publication publication;
    private Observer<Frame> observer;
    private Observable<Frame> observable;
    private ManyToManyConcurrentArrayQueue<FrameHolder> framesSendQueue;
    private Scheduler.Worker worker;

    public AeronClientDuplexConnection(Publication publication, ManyToManyConcurrentArrayQueue<FrameHolder> framesSendQueue) {
        this.publication = publication;
        this.framesSendQueue = framesSendQueue;
        this.observable = (Observer<Frame> o) -> {
            observer = o;
            observer.onSubscribe(new EmptyDisposable());
        };
        this.worker = Schedulers.computation().createWorker();

    }

    public Observer<Frame> getSubscriber() {
        return observer;
    }

    public Observable<Frame> getInput() {
        return observable;
    }

    @Override
    public void addOutput(Publisher<Frame> o, Completable callback) {
        o.subscribe(new Subscriber<Frame>() {
            volatile boolean running = true;
            Subscription s;
            @Override
            public void onSubscribe(Subscription s) {
                this.s = s;
                s.request(framesSendQueue.remainingCapacity() + 1);
            }

            @Override
            public void onNext(final Frame frame) {
                final FrameHolder frameHolder =  FrameHolder.get(frame, publication);
                int limit = Constants.MULTI_THREADED_SPIN_LIMIT;
                while (running && !framesSendQueue.offer(frameHolder)) {
                    if (--limit < 0) {
                        worker.schedule(() ->  onNext(frame));
                    }
                }
                s.request(framesSendQueue.remainingCapacity() + 1);
            }

            @Override
            public void onError(Throwable t) {
                running = false;
                callback.error(t);
            }

            @Override
            public void onComplete() {
                running = false;
                callback.success();
            }
        });
    }

    @Override
    public void close() {
        worker.unsubscribe();
    }
}
