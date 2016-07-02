package io.reactivesocket.util;

import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.ReactiveSocketFactory;
import org.reactivestreams.Publisher;

/**
 * A simple implementation that just forwards all methods to a passed child {@code ReactiveSocketFactory}.
 *
 * @param <T> Type parameter for {@link ReactiveSocketFactory}
 */
public abstract class ReactiveSocketFactoryProxy<T> implements ReactiveSocketFactory<T> {
    protected final ReactiveSocketFactory<T> child;

    protected ReactiveSocketFactoryProxy(ReactiveSocketFactory<T> child) {
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

    @Override
    public T remote() {
        return child.remote();
    }
}
