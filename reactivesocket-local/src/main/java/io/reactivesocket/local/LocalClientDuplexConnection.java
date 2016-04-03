package io.reactivesocket.local;

import io.reactivesocket.DuplexConnection;
import io.reactivesocket.Frame;
import io.reactivesocket.rx.Completable;
import io.reactivesocket.rx.Observable;
import io.reactivesocket.rx.Observer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;

class LocalClientDuplexConnection implements DuplexConnection {
    private final String name;

    private final CopyOnWriteArrayList<Observer<Frame>> subjects;

    public LocalClientDuplexConnection(String name) {
        this.name = name;
        this.subjects = new CopyOnWriteArrayList<>();
    }

    @Override
    public Observable<Frame> getInput() {
        return o -> {
            o.onSubscribe(() -> subjects.removeIf(s -> s == o));
            subjects.add(o);
        };
    }

    @Override
    public void addOutput(Publisher<Frame> o, Completable callback) {

        o
            .subscribe(new Subscriber<Frame>() {

                @Override
                public void onSubscribe(Subscription s) {
                    s.request(Long.MAX_VALUE);
                }

                @Override
                public void onNext(Frame frame) {
                    try {
                        LocalReactiveSocketManager
                            .getInstance()
                            .getServerConnection(name)
                            .write(frame);
                    } catch (Throwable t) {
                        onError(t);
                    }
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

    void write(Frame frame) {
        subjects
            .forEach(o -> o.onNext(frame));
    }

    @Override
    public void close() throws IOException {
        LocalReactiveSocketManager
            .getInstance()
            .removeClientConnection(name);

    }
}
