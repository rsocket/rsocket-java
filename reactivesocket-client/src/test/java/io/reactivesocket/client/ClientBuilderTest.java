package io.reactivesocket.client;

import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.ReactiveSocketConnector;
import io.reactivesocket.internal.Publishers;
import org.hamcrest.MatcherAssert;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import rx.Observable;
import rx.observers.TestSubscriber;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static org.hamcrest.Matchers.instanceOf;
import static rx.RxReactiveStreams.toObservable;

public class ClientBuilderTest {

    @Test(timeout = 10_000L)
    public void testIllegalState() throws ExecutionException, InterruptedException {
        // you need to specify the source and the connector
        Publisher<ReactiveSocket> socketPublisher = ClientBuilder.instance().build();
        Observable<ReactiveSocket> socketObservable = toObservable(socketPublisher);
        TestSubscriber<? super ReactiveSocket> testSubscriber = TestSubscriber.create();

        socketObservable.subscribe(testSubscriber);
        testSubscriber.awaitTerminalEvent();

        testSubscriber.assertNoValues();
        testSubscriber.assertError(IllegalStateException.class);
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

        Observable<ReactiveSocket> socketObservable = toObservable(socketPublisher);
        TestSubscriber<? super ReactiveSocket> testSubscriber = TestSubscriber.create();
        socketObservable.subscribe(testSubscriber);
        testSubscriber.awaitTerminalEvent();

        testSubscriber.assertNoErrors();
        testSubscriber.assertValueCount(1);
        testSubscriber.assertCompleted();

        ReactiveSocket socket = (ReactiveSocket) testSubscriber.getOnNextEvents().get(0);
        if (socket.availability() == 0.0) {
            throw new AssertionError("Loadbalancer availability is zero!");
        }
    }
}
