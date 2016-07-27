package io.reactivesocket.util;

import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.rx.Completable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class Unsafe {
    public static ReactiveSocket startAndWait(ReactiveSocket rsc) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        Completable completable = new Completable() {
            @Override
            public void success() {
                latch.countDown();
            }

            @Override
            public void error(Throwable e) {
                latch.countDown();
            }
        };
        rsc.start(completable);
        latch.await();

        return rsc;
    }

    public static ReactiveSocket awaitAvailability(ReactiveSocket rsc) throws InterruptedException {
        long waiting = 1L;
        while (rsc.availability() == 0.0) {
            Thread.sleep(waiting);
            waiting = Math.max(waiting * 2, 1000L);
        }
        return rsc;
    }

    public static <T> T blockingSingleWait(Publisher<T> publisher, long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
        return toSingleFuture(publisher).get(timeout, unit);
    }

    public static <T> List<T> blockingWait(Publisher<T> publisher, long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
        return toFuture(publisher).get(timeout, unit);
    }

    public static <T> CompletableFuture<T> toSingleFuture(Publisher<T> publisher) {
        return toFuture(publisher).thenApply(list -> list.get(0));
    }

    public static <T> CompletableFuture<List<T>> toFuture(Publisher<T> publisher) {
        CompletableFuture<List<T>> future = new CompletableFuture<>();

        publisher.subscribe(new Subscriber<T>() {
            private List<T> buffer = new ArrayList<T>();

            @Override
            public void onSubscribe(Subscription s) {
                s.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(T t) {
                buffer.add(t);
            }

            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(t);
            }

            @Override
            public void onComplete() {
                future.complete(buffer);
            }
        });

        return future;
    }
}
