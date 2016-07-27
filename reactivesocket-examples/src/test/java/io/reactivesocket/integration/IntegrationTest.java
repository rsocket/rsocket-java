package io.reactivesocket.integration;

import io.reactivesocket.*;
import io.reactivesocket.client.ClientBuilder;
import io.reactivesocket.internal.Publishers;
import io.reactivesocket.test.TestUtil;
import io.reactivesocket.transport.tcp.client.TcpReactiveSocketConnector;
import io.reactivesocket.transport.tcp.server.TcpReactiveSocketServer;
import io.reactivesocket.util.Unsafe;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static rx.RxReactiveStreams.toObservable;

public class IntegrationTest {

    private interface TestingServer {
        int requestCount();
        int disconnectionCount();
        SocketAddress getListeningAddress();
    }

    private TestingServer createServer() {
        AtomicInteger requestCounter = new AtomicInteger();
        AtomicInteger disconnectionCounter = new AtomicInteger();

        ConnectionSetupHandler setupHandler = (setupPayload, reactiveSocket) -> {
            reactiveSocket.onClose().subscribe(new Subscriber<Void>() {
                @Override
                public void onSubscribe(Subscription s) {
                    s.request(Long.MAX_VALUE);
                }

                @Override
                public void onNext(Void aVoid) {}

                @Override
                public void onError(Throwable t) {}

                @Override
                public void onComplete() {
                    disconnectionCounter.incrementAndGet();
                }
            });
            return new RequestHandler.Builder()
                .withRequestResponse(
                    payload -> subscriber -> subscriber.onSubscribe(new Subscription() {
                        @Override
                        public void request(long n) {
                            requestCounter.incrementAndGet();
                            subscriber.onNext(TestUtil.utf8EncodedPayload("RESPONSE", "NO_META"));
                            subscriber.onComplete();
                        }

                        @Override
                        public void cancel() {}
                    })
                )
                .build();
        };

        SocketAddress addr = new InetSocketAddress("127.0.0.1", 0);
        TcpReactiveSocketServer.StartedServer server =
            TcpReactiveSocketServer.create(addr).start(setupHandler);

        return new TestingServer() {
            @Override
            public int requestCount() {
                return requestCounter.get();
            }

            @Override
            public int disconnectionCount() {
                return disconnectionCounter.get();
            }

            @Override
            public SocketAddress getListeningAddress() {
                return server.getServerAddress();
            }
        };
    }

    private ReactiveSocket createClient(SocketAddress addr) throws InterruptedException, ExecutionException, TimeoutException {
        List<SocketAddress> addrs = Collections.singletonList(addr);
        Publisher<List<SocketAddress>> src = Publishers.just(addrs);

        ConnectionSetupPayload setupPayload =
            ConnectionSetupPayload.create("UTF-8", "UTF-8", ConnectionSetupPayload.HONOR_LEASE);
        TcpReactiveSocketConnector tcp = TcpReactiveSocketConnector.create(setupPayload, Throwable::printStackTrace);

        Publisher<ReactiveSocket> socketPublisher =
            ClientBuilder.<SocketAddress>instance()
                .withSource(src)
                .withConnector(tcp)
                .build();

        return Unsafe.blockingSingleWait(socketPublisher, 5, TimeUnit.SECONDS);
    }

    @Test(timeout = 2_000L)
    public void testRequest() throws ExecutionException, InterruptedException, TimeoutException {
        TestingServer server = createServer();
        ReactiveSocket client = createClient(server.getListeningAddress());

        toObservable(client.requestResponse(TestUtil.utf8EncodedPayload("RESPONSE", "NO_META")))
            .toBlocking()
            .subscribe();
        assertTrue("Server see the request", server.requestCount() > 0);
    }

    @Test(timeout = 2_000L)
    public void testClose() throws ExecutionException, InterruptedException, TimeoutException {
        TestingServer server = createServer();
        ReactiveSocket client = createClient(server.getListeningAddress());

        toObservable(client.close()).toBlocking().subscribe();

        Thread.sleep(100);
        assertTrue("Server see disconnection", server.disconnectionCount() > 0);
    }
}
