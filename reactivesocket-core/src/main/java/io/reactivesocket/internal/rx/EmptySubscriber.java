package io.reactivesocket.internal.rx;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public enum EmptySubscriber implements Subscriber<Object> {
    INSTANCE();

    @Override
    public void onSubscribe(Subscription s) {

    }

    @Override
    public void onNext(Object t) {}

    @Override
    public void onError(Throwable t) {}

    @Override
    public void onComplete() {}
}
