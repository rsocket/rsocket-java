package io.reactivesocket.aeron.client;

import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.aeron.internal.Loggable;
import io.reactivesocket.rx.Completable;
import io.reactivesocket.rx.Disposable;
import io.reactivesocket.rx.Observable;
import io.reactivesocket.rx.Observer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.agrona.concurrent.AbstractConcurrentArrayQueue;

import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

public class AeronClientDuplexConnection implements DuplexConnection, Loggable {

    private final Publication publication;
    private final CopyOnWriteArrayList<Observer<Frame>> subjects;
    private final AbstractConcurrentArrayQueue<FrameHolder> frameSendQueue;
    private final Consumer<Publication> onClose;

    public AeronClientDuplexConnection(
        Publication publication,
        AbstractConcurrentArrayQueue<FrameHolder> frameSendQueue,
        Consumer<Publication> onClose) {
        this.publication = publication;
        this.subjects = new CopyOnWriteArrayList<>();
        this.frameSendQueue = frameSendQueue;
        this.onClose = onClose;
    }

    @Override
    public final Observable<Frame> getInput() {
        if (isTraceEnabled()) {
            trace("getting input for publication session id {} ", publication.sessionId());
        }

        return new Observable<Frame>() {
            public void subscribe(Observer<Frame> o) {
                o.onSubscribe(new Disposable() {
                    @Override
                    public void dispose() {
                        if (isTraceEnabled()) {
                            trace("removing Observer for publication with session id {} ", publication.sessionId());
                        }

                        subjects.removeIf(s -> s == o);
                    }
                });

                subjects.add(o);
            }
        };
    }

    @Override
    public void addOutput(Publisher<Frame> o, Completable callback) {
        o
            .subscribe(new Subscriber<Frame>() {
                private Subscription s;

                @Override
                public void onSubscribe(Subscription s) {
                    this.s = s;
                    s.request(128);

                }

                @Override
                public void onNext(Frame frame) {
                    if (isTraceEnabled()) {
                        trace("onNext subscription => {} and frame => {}", s.toString(), frame.toString());
                    }

                    final FrameHolder fh = FrameHolder.get(frame, publication, s);
                    boolean offer;
                    do {
                        offer = frameSendQueue.offer(fh);
                    } while (!offer);
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
    public void close() throws IOException {
        onClose.accept(publication);
    }

    public CopyOnWriteArrayList<Observer<Frame>> getSubjects() {
        return subjects;
    }


}
