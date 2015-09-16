package io.reactivesocket.aeron.client;

import io.reactivesocket.Completable;
import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.internal.EmptyDisposable;
import io.reactivesocket.observable.Observable;
import io.reactivesocket.observable.Observer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.agrona.concurrent.ManyToOneConcurrentArrayQueue;

public class AeronClientDuplexConnection implements DuplexConnection, AutoCloseable {
    private Publication publication;
    private Observer<Frame> observer;
    private Observable<Frame> observable;
    private ManyToOneConcurrentArrayQueue<FrameHolder> framesSendQueue;

    public AeronClientDuplexConnection(Publication publication, ManyToOneConcurrentArrayQueue<FrameHolder> framesSendQueue) {

        System.out.println("publication => " + publication.toString());
        this.publication = publication;
        this.framesSendQueue = framesSendQueue;
        this.observable = (Observer<Frame> o) -> {
            observer = o;
            observer.onSubscribe(new EmptyDisposable());
        };

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
                s.request(1);
            }

            @Override
            public void onNext(Frame frame) {
                final FrameHolder frameHolder =  FrameHolder.get(frame, publication);
                while (running && !framesSendQueue.offer(frameHolder)) {

                }
                s.request(1);
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
    }
}

