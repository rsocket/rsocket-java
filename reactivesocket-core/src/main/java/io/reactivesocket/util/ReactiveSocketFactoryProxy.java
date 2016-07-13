package io.reactivesocket.util;

import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.ReactiveSocketFactory;
import org.reactivestreams.Publisher;

/**
 * A simple implementation that just forwards all methods to a passed child {@code ReactiveSocketFactory}.
 */
public abstract class ReactiveSocketFactoryProxy implements ReactiveSocketFactory {
    protected final ReactiveSocketFactory child;

    protected ReactiveSocketFactoryProxy(ReactiveSocketFactory child) {
        this.child = child;
    }

    @Override
    public Publisher<ReactiveSocket> apply() {
        return child.apply();
    }

    @Override
    public double availability() {
        return child.availability();
    }

}
