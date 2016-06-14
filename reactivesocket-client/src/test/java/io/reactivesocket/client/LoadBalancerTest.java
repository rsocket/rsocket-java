package io.reactivesocket.client;

import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.ReactiveSocketFactory;
import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

public class LoadBalancerTest {

    private Payload dummy = new Payload() {
        @Override
        public ByteBuffer getData() {
            return null;
        }

        @Override
        public ByteBuffer getMetadata() {
            return null;
        }
    };

    @Test(timeout = 10_000L)
    public void testNeverSelectFailingFactories() throws InterruptedException {
        InetSocketAddress local0 = InetSocketAddress.createUnresolved("localhost", 7000);
        InetSocketAddress local1 = InetSocketAddress.createUnresolved("localhost", 7001);

        TestingReactiveSocket socket = new TestingReactiveSocket(Function.identity());
        ReactiveSocketFactory<SocketAddress> failing = failingFactory(local0);
        ReactiveSocketFactory<SocketAddress> succeeding = succeedingFactory(local1, socket);
        List<ReactiveSocketFactory<SocketAddress>> factories = Arrays.asList(failing, succeeding);

        testBalancer(factories);
    }

    @Test(timeout = 10_000L)
    public void testNeverSelectFailingSocket() throws InterruptedException {
        InetSocketAddress local0 = InetSocketAddress.createUnresolved("localhost", 7000);
        InetSocketAddress local1 = InetSocketAddress.createUnresolved("localhost", 7001);

        TestingReactiveSocket socket = new TestingReactiveSocket(Function.identity());
        TestingReactiveSocket failingSocket = new TestingReactiveSocket(Function.identity()) {
            @Override
            public Publisher<Payload> requestResponse(Payload payload) {
                return subscriber ->
                    subscriber.onError(new RuntimeException("You shouldn't be here"));
            }

            public double availability() {
                return 0.0;
            }
        };

        ReactiveSocketFactory<SocketAddress> failing = succeedingFactory(local0, failingSocket);
        ReactiveSocketFactory<SocketAddress> succeeding = succeedingFactory(local1, socket);
        List<ReactiveSocketFactory<SocketAddress>> factories = Arrays.asList(failing, succeeding);

        testBalancer(factories);
    }

    private void testBalancer(List<ReactiveSocketFactory<SocketAddress>> factories) throws InterruptedException {
        Publisher<List<ReactiveSocketFactory<SocketAddress>>> src = s -> {
            s.onNext(factories);
            s.onComplete();
        };

        LoadBalancer balancer = new LoadBalancer(src);

        while (balancer.availability() == 0.0) {
            Thread.sleep(1);
        }

        for (int i = 0; i < 100; i++) {
            makeAcall(balancer);
        }
    }

    private void makeAcall(ReactiveSocket balancer) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);

        balancer.requestResponse(dummy).subscribe(new Subscriber<Payload>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(1L);
            }

            @Override
            public void onNext(Payload payload) {
                System.out.println("Successfully receiving a response");
            }

            @Override
            public void onError(Throwable t) {
                t.printStackTrace();
                Assert.assertTrue(false);
                latch.countDown();
            }

            @Override
            public void onComplete() {
                latch.countDown();
            }
        });

        latch.await();
    }

    private ReactiveSocketFactory<SocketAddress> succeedingFactory(SocketAddress sa, ReactiveSocket socket) {
        return new ReactiveSocketFactory<SocketAddress>() {
            @Override
            public Publisher<ReactiveSocket> apply() {
                return s -> s.onNext(socket);
            }

            @Override
            public double availability() {
                return 1.0;
            }

            @Override
            public SocketAddress remote() {
                return sa;
            }
        };
    }

    private ReactiveSocketFactory<SocketAddress> failingFactory(SocketAddress sa) {
        return new ReactiveSocketFactory<SocketAddress>() {
            @Override
            public Publisher<ReactiveSocket> apply() {
                Assert.assertTrue(false);
                return null;
            }

            @Override
            public double availability() {
                return 0.0;
            }

            @Override
            public SocketAddress remote() {
                return sa;
            }
        };
    }
}
