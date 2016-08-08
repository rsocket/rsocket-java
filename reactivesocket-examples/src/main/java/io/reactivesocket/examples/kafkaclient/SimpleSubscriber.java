package io.reactivesocket.examples.kafkaclient;

import io.reactivesocket.Payload;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.function.Consumer;

/**
 * This class may look extremely confusing at first, especially if one is not familiar with Java 8 functional
 * programming, but the idea behind it is simple. The class SimpleSubscriber implements a Subscriber interface,
 * and the nested Builder class allows one to pass in Objects that define some function to set the different handlers
 * for the Subscriber
 *
 * The idea for this design is that it allows us to cleanly pass in anonymous functions to define the handlers that
 * we want without having to construct big anonymous classes or have messy boilerplate code.
 */
public abstract class SimpleSubscriber implements Subscriber<Payload> {


    /**
     * This class allows us to build a Subscriber object by passing in Objects that contain functions
     * The handlers we don't pass in are defined by the default behavior.
     */
    public static class Builder {
        private Consumer<Subscription> handleOnSubscribe = DEFAULT_ON_SUBSCRIBE;
        private Consumer<Payload> handleOnNext = DEFAULT_ON_NEXT;
        private Consumer<Throwable> handleOnError = DEFAULT_ON_ERROR;
        private Runnable handleOnComplete = DEFAULT_ON_COMPLETE;

        public Builder withOnSubscribe(final Consumer<Subscription> onSub) {
            this.handleOnSubscribe = onSub;
            return this;
        }

        public Builder withOnNext(final Consumer<Payload> onNext) {
            this.handleOnNext = onNext;
            return this;
        }

        public Builder withOnError(final Consumer<Throwable> onError) {
            this.handleOnError = onError;
            return this;
        }

        public Builder withOnComplete(final Runnable onComplete) {
            this.handleOnComplete = onComplete;
            return this;
        }

        /**
         * THis function returns the subscriber that we have constructed using our Builder object
         * @return
         */
        public SimpleSubscriber build() {
            return new SimpleSubscriber() {
                @Override
                public void onSubscribe(Subscription s) {
                    handleOnSubscribe.accept(s);
                }

                @Override
                public void onNext(Payload payload) {
                    handleOnNext.accept(payload);
                }

                @Override
                public void onError(Throwable t) {
                    handleOnError.accept(t);
                }

                @Override
                public void onComplete() {
                    handleOnComplete.run();
                }
            };
        }

        private static final Consumer<Subscription> DEFAULT_ON_SUBSCRIBE = new Consumer<Subscription>() {
            @Override
            public void accept(Subscription subscription) {
                subscription.request(Long.MAX_VALUE);
            }
        };

        private static final Consumer<Payload> DEFAULT_ON_NEXT = new Consumer<Payload>() {
            @Override
            public void accept(Payload payload) { }
        };

        private static final Consumer<Throwable> DEFAULT_ON_ERROR = new Consumer<Throwable>() {
            @Override
            public void accept(Throwable throwable) {
                System.out.println(throwable.getStackTrace());
            }
        };

        private static final Runnable DEFAULT_ON_COMPLETE = new Runnable() {
            @Override
            public void run() { }
        };
    }


}
