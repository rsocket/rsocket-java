package io.reactivesocket.netty.websocket.client;

import io.netty.channel.EventLoopGroup;
import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.ReactiveSocketFactory;
import io.reactivesocket.rx.Completable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.RxReactiveStreams;

import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * An implementation of {@link ReactiveSocketFactory} that creates Netty WebSocket ReactiveSockets.
 */
public class WebSocketReactiveSocketFactory implements ReactiveSocketFactory {
    private static final Logger logger = LoggerFactory.getLogger(WebSocketReactiveSocketFactory.class);

    private final ConnectionSetupPayload connectionSetupPayload;
    private final Consumer<Throwable> errorStream;
    private final String path;
    private final EventLoopGroup eventLoopGroup;

    public WebSocketReactiveSocketFactory(String path, EventLoopGroup eventLoopGroup, ConnectionSetupPayload connectionSetupPayload, Consumer<Throwable> errorStream) {
        this.connectionSetupPayload = connectionSetupPayload;
        this.errorStream = errorStream;
        this.path = path;
        this.eventLoopGroup = eventLoopGroup;
    }

    @Override
    public Publisher<ReactiveSocket> call(SocketAddress address, long timeout, TimeUnit timeUnit) {
        Publisher<ClientWebSocketDuplexConnection> connection
                = ClientWebSocketDuplexConnection.create(address, path, eventLoopGroup);

        Observable<ReactiveSocket> result = Observable.create(s ->
            connection.subscribe(new Subscriber<ClientWebSocketDuplexConnection>() {
                @Override
                public void onSubscribe(Subscription s) {
                    s.request(1);
                }

                @Override
                public void onNext(ClientWebSocketDuplexConnection connection) {
                    ReactiveSocket reactiveSocket = ReactiveSocket.fromClientConnection(connection, connectionSetupPayload, errorStream);
                    reactiveSocket.start(new Completable() {
                        @Override
                        public void success() {
                            s.onNext(reactiveSocket);
                            s.onCompleted();
                        }

                        @Override
                        public void error(Throwable e) {
                            s.onError(e);
                        }
                    });
                }

                @Override
                public void onError(Throwable t) {
                    s.onError(t);
                }

                @Override
                public void onComplete() {
                }
            })
        );

        return RxReactiveStreams.toPublisher(result.timeout(timeout, timeUnit));
    }
}
