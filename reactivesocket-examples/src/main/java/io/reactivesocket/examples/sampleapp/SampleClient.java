package io.reactivesocket.examples.sampleapp;

import io.netty.buffer.ByteBuf;

import io.reactivesocket.*;
import io.reactivesocket.transport.tcp.client.TcpReactiveSocketConnector;
import io.reactivesocket.util.PayloadImpl;
import io.reactivex.netty.protocol.tcp.client.TcpClient;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.net.*;
import java.util.function.Function;

import static rx.RxReactiveStreams.toObservable;


/**
 * This is an example ReactiveSocket client, and does not follow the reactive streams protocol. The purpose of this
 * class is to demonstrate how to create a client and how to launce interactions with a ReactiveSocket server.
 */
public class SampleClient {
    public static void main(String[] args) throws MalformedURLException, URISyntaxException {
        String target = args.length > 0 ? args[0] : "tcp://localhost:4567/rs";
        URI uri = new URI(target);

        ReactiveSocket client = createClient(uri);

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
                s.request(5);
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
        fnfPublisher.subscribe(fnfsubscriber);
        rrPublisher.subscribe(rrsubscriber);
        cPublisher.subscribe(csubscriber);
        rsPublisher.subscribe(rrsubscriber);

    }

    /**
     * A function that creates a ReactiveSocket on a new TCP connection.
     * @return a ReactiveSocket
     */
    public static ReactiveSocket createClient(URI uri) throws URISyntaxException {
        ConnectionSetupPayload setupPayload = ConnectionSetupPayload.create("", "");
        if ("tcp".equals(uri.getScheme())) {
            Function<SocketAddress, TcpClient<ByteBuf, ByteBuf>> clientFactory =
                    socketAddress -> TcpClient.newClient(socketAddress);

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
