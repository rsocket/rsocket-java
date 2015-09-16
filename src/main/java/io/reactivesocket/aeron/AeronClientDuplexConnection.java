package io.reactivesocket.aeron;

import io.reactivesocket.Completable;
import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.FrameType;
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

    public AeronClientDuplexConnection(Publication publication, ManyToOneConcurrentArrayQueue<FrameHolder> framesSendQueue, AtomicBoolean initialized) {

        System.out.println("publication => " + publication.toString());
        this.publication = publication;
        this.framesSendQueue = framesSendQueue;
        this.observable = new Observable<Frame>() {
            @Override
            public void subscribe(Observer<Frame> o) {
                 observer = o;
            }
        };

        this.initialized = initialized;

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
            @Override
            public void onSubscribe(Subscription s) {
                s.request(1);
            }

            @Override
            public void onNext(Frame frame) {
                System.out.println("STARTED => " + initialized.get());
                if (frame.getType() != FrameType.SETUP) {
                    System.out.println("dropping frame that isn't setup => " + frame.toString());
                } else {
                    System.out.println("#### #### #### FOUND THE SETUP FRAME => " + frame.toString());
                }

                //while (!framesSendQueue.offer(new FrameHolder(frame, publication))) {}
            }

            @Override
            public void onError(Throwable t) {
                callback.error(t);
            }

            @Override
            public void onComplete() {
                callback.success();
            }
        });
    }

    @Override
    public void close() {
        observer.onComplete();
    }
}

