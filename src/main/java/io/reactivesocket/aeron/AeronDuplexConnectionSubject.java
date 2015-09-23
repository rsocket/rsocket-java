package io.reactivesocket.aeron;

import io.reactivesocket.Frame;
import io.reactivesocket.rx.Disposable;
import io.reactivesocket.rx.Observable;
import io.reactivesocket.rx.Observer;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
* Class used to manage connections input to a duplex connection in Aeron.
*/
public class AeronDuplexConnectionSubject implements Observable<Frame>, Observer<Frame> {
    private static final AtomicLong count = new AtomicLong();

    private final long id;

    private final List<AeronDuplexConnectionSubject> subjects;

    private Observer<Frame> internal;

    public AeronDuplexConnectionSubject(List<AeronDuplexConnectionSubject> subjects) {
        this.id = count.incrementAndGet();
        this.subjects = subjects;
    }

    @Override
    public void subscribe(Observer<Frame> o) {
        internal = o;
        internal.onSubscribe(() ->
                subjects
                    .removeIf(s -> s.id == id));
    }

    @Override
    public void onNext(Frame frame) {
        internal.onNext(frame);
    }

    @Override
    public void onError(Throwable e) {
        internal.onError(e);
    }

    @Override
    public void onComplete() {
        internal.onComplete();
    }

    @Override
    public void onSubscribe(Disposable d) {

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AeronDuplexConnectionSubject that = (AeronDuplexConnectionSubject) o;

        if (id != that.id) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return (int) (id ^ (id >>> 32));
    }
}
