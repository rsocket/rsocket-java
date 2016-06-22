package io.reactivesocket.util;

import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.ReactiveSocketFactory;
import org.reactivestreams.Publisher;

/**
 * A simple implementation that just forwards all methods to a passed delegate {@code ReactiveSocketFactory}.
 *
 * @param <T> Type parameter for {@link ReactiveSocketFactory}
 */
public abstract class ReactiveSocketFactoryProxy<T> implements ReactiveSocketFactory<T> {

    private final ReactiveSocketFactory<T> delegate;

    protected ReactiveSocketFactoryProxy(ReactiveSocketFactory<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public Publisher<ReactiveSocket> apply() {
        return delegate.apply();
    }

    @Override
    public double availability() {
        return delegate.availability();
    }

    @Override
    public T remote() {
        return delegate.remote();
    }
}
