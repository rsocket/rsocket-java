package io.reactivesocket.client;

import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.ReactiveSocketConnector;
import io.reactivesocket.internal.Publishers;
import org.hamcrest.MatcherAssert;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static org.hamcrest.Matchers.instanceOf;

public class ClientBuilderTest {

    @Test(timeout = 10_000L)
    public void testIllegalState() throws ExecutionException, InterruptedException {
        // you need to specify the source and the connector
        Publisher<ReactiveSocket> socketPublisher = ClientBuilder.instance().build();

        CountDownLatch latch = new CountDownLatch(1);
        socketPublisher.subscribe(new Subscriber<ReactiveSocket>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(1L);
            }

            @Override
            public void onNext(ReactiveSocket reactiveSocket) {
                throw new AssertionError("onNext invoked when not expected.");
            }

            @Override
            public void onError(Throwable t) {
                MatcherAssert.assertThat("Unexpected exception in onError", t, instanceOf(IllegalStateException.class));
                latch.countDown();
            }

            @Override
            public void onComplete() {
                throw new AssertionError("onComplete invoked when not expected.");
            }
        });

        latch.await();
    }

    @Test(timeout = 10_000L)
    public void testReturnedRSisAvailable() throws ExecutionException, InterruptedException {

        List<SocketAddress> addrs = Collections.singletonList(
            InetSocketAddress.createUnresolved("localhost", 8080));
        Publisher<List<SocketAddress>> src = Publishers.just(addrs);

        ReactiveSocketConnector<SocketAddress> connector =
            address -> Publishers.just(new TestingReactiveSocket(Function.identity()));

        Publisher<ReactiveSocket> socketPublisher =
            ClientBuilder.<SocketAddress>instance()
                .withSource(src)
                .withConnector(connector)
                .build();

        CountDownLatch latch = new CountDownLatch(1);
        socketPublisher.subscribe(new Subscriber<ReactiveSocket>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(1L);
            }

            @Override
            public void onNext(ReactiveSocket reactiveSocket) {
                // the returned ReactiveSocket must have an availability > 0.0
                if (reactiveSocket.availability() == 0.0) {
                    throw new AssertionError("Loadbalancer availability is zero!");
                }
            }

            @Override
            public void onError(Throwable t) {
                throw new AssertionError("onError invoked when not expected.");
            }

            @Override
            public void onComplete() {
                latch.countDown();
            }
        });

        latch.await();
    }
}
