package io.reactivesocket.tckdrivers.test;

import io.netty.buffer.ByteBuf;
import io.netty.handler.logging.LogLevel;
import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.transport.tcp.client.TcpReactiveSocketConnector;
import io.reactivesocket.util.PayloadImpl;
import io.reactivex.netty.protocol.tcp.client.TcpClient;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.net.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static rx.RxReactiveStreams.toObservable;

public class Main {
    public static void main(String[] args) throws MalformedURLException, URISyntaxException, InterruptedException {
        String target = args.length > 0 ? args[0] : "tcp://localhost:4567/rs";
        URI uri = new URI(target);

        ReactiveSocket client = buildConnectionAndSocket(uri);
        CountDownLatch count = new CountDownLatch(1);
        rx.Completable c = rx.Completable.complete();

        rx.Completable run = run(client);
        run.await();
    }

    public static rx.Completable run(ReactiveSocket client) throws InterruptedException {
        CountDownLatch count = new CountDownLatch(1);
        // trivial publisher for fire n forget
        Publisher<Void> fnfPublisher = client.fireAndForget(new PayloadImpl("abcd", "abcd"));

        // publisher for request response
        Publisher<Payload> rrPublisher = client.requestResponse(new PayloadImpl("a", "b"));

        // publisher for request stream
        Publisher<Payload> rsPublisher = client.requestStream(new PayloadImpl("worldsta", "worldsta"));

        // publisher for channel, it takes in a publisher
        Publisher<Payload> cPublisher = client.requestChannel(new Publisher<Payload>() {
            @Override
            public void subscribe(Subscriber<? super Payload> s) {
                s.onSubscribe(new Subscription() {
                    @Override
                    public void request(long n) {
                        s.onNext(new PayloadImpl("e", "f"));
                        s.onNext(new PayloadImpl("a", "a"));
                        s.onNext(new PayloadImpl("a", "a"));
                        s.onNext(new PayloadImpl("a", "a"));
                        s.onNext(new PayloadImpl("a", "a"));
                        s.onNext(new PayloadImpl("a", "a"));
                        System.out.println("data requested " + n);
                        s.onComplete();
                    }

                    @Override
                    public void cancel() {
                        return;
                    }
                });
            }
        });

        // subscriber for requestresponse
        Subscriber<? super Payload> rrsubscriber = new Subscriber<Payload>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(100);
            }

            @Override
            public void onNext(Payload payload) {
                System.out.println("server responded with the following: " + payload);
            }

            @Override
            public void onError(Throwable t) {
                t.printStackTrace();
            }

            @Override
            public void onComplete() {
                System.out.println("finished");
            }
        };

        // subscriber for fire n forget
        Subscriber<Void> fnfsubscriber = new Subscriber<Void>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(Void aVoid) {
                return;
            }

            @Override
            public void onError(Throwable t) {
                t.printStackTrace();
            }

            @Override
            public void onComplete() {
                System.out.println("fired and forgot");
            }
        };

        // subscriber for channel
        Subscriber<Payload> csubscriber = new Subscriber<Payload>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(3);
            }

            @Override
            public void onNext(Payload payload) {
                System.out.println("channel payload: " + payload);
            }

            @Override
            public void onError(Throwable t) {
                t.printStackTrace();
            }

            @Override
            public void onComplete() {
                System.out.println("channel finished");
            }
        };

        // We start the connection for each of our subscribers
        //fnfPublisher.subscribe(fnfsubscriber);
        //RxReactiveStreams.toPublisher(toObservable(rrPublisher)).subscribe(rrsubscriber);
        rrPublisher.subscribe(rrsubscriber);
        rrPublisher.subscribe(rrsubscriber);
        cPublisher.subscribe(csubscriber);
        rsPublisher.subscribe(rrsubscriber);
        return rx.Completable.timer(5000, TimeUnit.MILLISECONDS);
    }

    private static Publisher<ReactiveSocket> buildConnection(URI uri) {
        if (uri.getScheme().equals("tcp")) {
            ConnectionSetupPayload setupPayload = ConnectionSetupPayload.create("UTF-8", "UTF-8", ConnectionSetupPayload.NO_FLAGS);

            TcpReactiveSocketConnector tcp = TcpReactiveSocketConnector.create(setupPayload, Throwable::printStackTrace);
            Publisher<ReactiveSocket> socketPublisher = tcp.connect(new InetSocketAddress("localhost", 4567));
            return socketPublisher;

        } else {
            throw new UnsupportedOperationException("uri unsupported: " + uri);
        }
    }

    public static ReactiveSocket createClient() {
        try {
            String target = "tcp://localhost:4567/rs";
            URI uri = new URI(target);

            Publisher<ReactiveSocket> socketPublisher = buildConnection(uri);
            SocketWrapper client = new SocketWrapper();
            CountDownLatch hold = new CountDownLatch(1);
            socketPublisher.subscribe(new Subscriber<ReactiveSocket>() {
                @Override
                public void onSubscribe(Subscription s) {
                    s.request(1);
                }

                @Override
                public void onNext(ReactiveSocket reactiveSocket) {
                    client.setSocket(reactiveSocket);
                    hold.countDown();
                }

                @Override
                public void onError(Throwable t) {
                    System.out.println("error when getting reactive socket");
                }

                @Override
                public void onComplete() {

                }
            });
            hold.await();
            return client.getSocket();
        } catch (Exception e) {
            System.out.println("Something went wrong");
        }
        return null;
    }

    private static class SocketWrapper {

        private ReactiveSocket rs;

        public void setSocket(ReactiveSocket rs) {
            this.rs = rs;
        }

        public ReactiveSocket getSocket() {
            return this.rs;
        }
    }

    public static ReactiveSocket buildConnectionAndSocket(URI uri) throws InterruptedException {
        ConnectionSetupPayload setupPayload = ConnectionSetupPayload.create("", "");

        if ("tcp".equals(uri.getScheme())) {
            Function<SocketAddress, TcpClient<ByteBuf, ByteBuf>> clientFactory =
                    socketAddress -> TcpClient.newClient(socketAddress).enableWireLogging("rs",
                            LogLevel.ERROR);
            return toObservable(
                    TcpReactiveSocketConnector.create(setupPayload, Throwable::printStackTrace, clientFactory)
                            .connect(new InetSocketAddress(uri.getHost(), uri.getPort()))).toSingle()
                    .toBlocking()
                    .value();
        }
         else {
            throw new UnsupportedOperationException("uri unsupported: " + uri);
        }
    }
}
