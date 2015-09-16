package io.reactivesocket.aeron;

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

import java.util.concurrent.atomic.AtomicBoolean;

public class AeronClientDuplexConnection implements DuplexConnection, AutoCloseable {
    private Publication publication;
    private Observer<Frame> observer;
    private Observable<Frame> observable;
    private ManyToOneConcurrentArrayQueue<FrameHolder> framesSendQueue;
    private AtomicBoolean initialized;

    public AeronClientDuplexConnection(Publication publication, ManyToOneConcurrentArrayQueue<FrameHolder> framesSendQueue) {

        System.out.println("publication => " + publication.toString());
        this.publication = publication;
        this.framesSendQueue = framesSendQueue;
        this.observable = new Observable<Frame>() {
            @Override
            public void subscribe(Observer<Frame> o) {
                 observer = o;
                observer.onSubscribe(new EmptyDisposable());
            }
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

            @Override
            public void onSubscribe(Subscription s) {
                s.request(128);
            }

            @Override
            public void onNext(Frame frame) {
                while (running && !framesSendQueue.offer(new FrameHolder(frame, publication))) {

                }
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

