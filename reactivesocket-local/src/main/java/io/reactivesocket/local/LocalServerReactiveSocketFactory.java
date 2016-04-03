package io.reactivesocket.local;

import io.reactivesocket.ConnectionSetupHandler;
import io.reactivesocket.DefaultReactiveSocket;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.ReactiveSocketFactory;
import io.reactivesocket.internal.rx.EmptySubscription;
import org.reactivestreams.Publisher;

public class LocalServerReactiveSocketFactory implements ReactiveSocketFactory<LocalServerReactiveSocketFactory.Config, ReactiveSocket> {
    public static final LocalServerReactiveSocketFactory INSTANCE = new LocalServerReactiveSocketFactory();

    private LocalServerReactiveSocketFactory() {}

    @Override
    public Publisher<ReactiveSocket> call(Config config) {
        return s -> {
            try {
                s.onSubscribe(EmptySubscription.INSTANCE);
                LocalServerDuplexConection clientConnection = LocalReactiveSocketManager
                    .getInstance()
                    .getServerConnection(config.getName());
                ReactiveSocket reactiveSocket = DefaultReactiveSocket
                    .fromServerConnection(clientConnection, config.getConnectionSetupHandler());

                reactiveSocket.startAndWait();
                s.onNext(reactiveSocket);
                s.onComplete();
            } catch (Throwable t) {
                s.onError(t);
            }
        };
    }

    public static class Config {
        final String name;
        final ConnectionSetupHandler connectionSetupHandler;

        public Config(String name, ConnectionSetupHandler connectionSetupHandler) {
            this.name = name;
            this.connectionSetupHandler = connectionSetupHandler;
        }

        public ConnectionSetupHandler getConnectionSetupHandler() {
            return connectionSetupHandler;
        }

        public String getName() {
            return name;
        }
    }
}
