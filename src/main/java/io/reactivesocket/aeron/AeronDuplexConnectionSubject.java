package io.reactivesocket.aeron;

import io.reactivesocket.Frame;
import io.reactivesocket.observable.Disposable;
import io.reactivesocket.observable.Observable;
import io.reactivesocket.observable.Observer;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

/**
* Class used to manage connections input to a duplex connection in Aeron.
*/
public class AeronDuplexConnectionSubject implements Observable<Frame>, Observer<Frame> {
    private static final AtomicLong count = new AtomicLong();

    private final long id;

    private final ArrayList<AeronDuplexConnectionSubject> subjects;

    private Observer<Frame> internal;

    public AeronDuplexConnectionSubject(ArrayList<AeronDuplexConnectionSubject> subjects) {
        this.id = count.incrementAndGet();
        this.subjects = subjects;
    }

    @Override
    public void subscribe(Observer<Frame> o) {
        internal = o;
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
        internal.onSubscribe(()->
            subjects
                .removeIf(new Predicate<AeronDuplexConnectionSubject>() {
                    @Override
                    public boolean test(AeronDuplexConnectionSubject subject) {
                        return id == subject.id;
                    }
                })
            );
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
