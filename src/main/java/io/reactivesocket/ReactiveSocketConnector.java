package io.reactivesocket;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

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
                return subscriber ->
                    ReactiveSocketConnector.this.connect(address).subscribe(new Subscriber<ReactiveSocket>() {
                        @Override
                        public void onSubscribe(Subscription s) {
                            subscriber.onSubscribe(s);
                        }

                        @Override
                        public void onNext(ReactiveSocket reactiveSocket) {
                            ReactiveSocket socket = func.apply(reactiveSocket);
                            subscriber.onNext(socket);
                        }

                        @Override
                        public void onError(Throwable t) {
                            subscriber.onError(t);
                        }

                        @Override
                        public void onComplete() {
                            subscriber.onComplete();
                        }
                    });
            }
        };
    }

    /**
     * Create a ReactiveSocketFactory from a ReactiveSocketConnector
     * @param address the address to connect the connector to
     * @return the factory
     */
    default ReactiveSocketFactory<T> toFactory(T address) {
        return new ReactiveSocketFactory<T>() {
            @Override
            public Publisher<ReactiveSocket> apply() {
                return ReactiveSocketConnector.this.connect(address);
            }

            @Override
            public double availability() {
                return 1.0;
            }

            @Override
            public T remote() {
                return address;
            }
        };
    }
}
