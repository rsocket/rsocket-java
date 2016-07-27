package io.reactivesocket;

import io.reactivesocket.internal.Publishers;
import org.reactivestreams.Publisher;

import java.util.function.Function;

@FunctionalInterface
public interface ReactiveSocketConnector<T> {
    /**
     * Asynchronously connect and construct a ReactiveSocket
     * @return a Publisher that will return the ReactiveSocket
     */
    Publisher<ReactiveSocket> connect(T address);

    /**
     * Transform the ReactiveSocket returned by the connector via the provided function `func`
     * @param func the transformative function
     * @return a new ReactiveSocketConnector
     */
    default ReactiveSocketConnector<T> chain(Function<ReactiveSocket, ReactiveSocket> func) {
        return new ReactiveSocketConnector<T>() {
            @Override
            public Publisher<ReactiveSocket> connect(T address) {
                return Publishers.map(ReactiveSocketConnector.this.connect(address), func);
            }
        };
    }

    /**
     * Create a ReactiveSocketFactory from a ReactiveSocketConnector
     * @param address the address to connect the connector to
     * @return the factory
     */
    default ReactiveSocketFactory toFactory(T address) {
        return new ReactiveSocketFactory() {
            @Override
            public Publisher<ReactiveSocket> apply() {
                return connect(address);
            }

            @Override
            public double availability() {
                return 1.0;
            }

        };
    }
}
