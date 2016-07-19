package io.reactivesocket.tckdrivers.test;

import io.netty.channel.nio.NioEventLoopGroup;
import io.reactivesocket.*;

import io.reactivesocket.rx.Completable;
import io.reactivesocket.tckdrivers.client.JavaTCPClient;
import io.reactivesocket.transport.tcp.client.TcpReactiveSocketConnector;
import io.reactivesocket.util.Unsafe;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import rx.RxReactiveStreams;

import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CountDownLatch;

import static java.net.InetSocketAddress.createUnresolved;

public class Main {
    public static void main(String[] args) throws MalformedURLException, URISyntaxException, InterruptedException {
        String target = args.length > 0 ? args[0] : "tcp://localhost:4567/rs";
        URI uri = new URI(target);

        ReactiveSocket client = createClient();

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
        Subscriber<Payload> rrsubscriber = new Subscriber<Payload>() {
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
        //rrPublisher.subscribe(rrsubscriber);
        //cPublisher.subscribe(csubscriber);
        rsPublisher.subscribe(rrsubscriber);

    }

    private static Publisher<ReactiveSocket> buildConnection(URI uri) {
        if (uri.getScheme().equals("tcp")) {
            ConnectionSetupPayload setupPayload = ConnectionSetupPayload.create("UTF-8", "UTF-8", ConnectionSetupPayload.HONOR_LEASE);

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
}
