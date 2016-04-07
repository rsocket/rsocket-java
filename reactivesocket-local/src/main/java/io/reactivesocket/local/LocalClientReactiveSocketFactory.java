package io.reactivesocket.local;

import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.DefaultReactiveSocket;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.ReactiveSocketFactory;
import io.reactivesocket.internal.rx.EmptySubscription;
import org.reactivestreams.Publisher;

public class LocalClientReactiveSocketFactory implements ReactiveSocketFactory<LocalClientReactiveSocketFactory.Config, ReactiveSocket> {
    public static final LocalClientReactiveSocketFactory INSTANCE = new LocalClientReactiveSocketFactory();

    private LocalClientReactiveSocketFactory() {}

    @Override
    public Publisher<ReactiveSocket> call(Config config) {
        return s -> {
            try {
                s.onSubscribe(EmptySubscription.INSTANCE);
                LocalClientDuplexConnection clientConnection = LocalReactiveSocketManager
                    .getInstance()
                    .getClientConnection(config.getName());
                ReactiveSocket reactiveSocket = DefaultReactiveSocket
                    .fromClientConnection(clientConnection, ConnectionSetupPayload.create(config.getMetadataMimeType(), config.getDataMimeType()));

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
        final String metadataMimeType;
        final String dataMimeType;

        public Config(String name, String metadataMimeType, String dataMimeType) {
            this.name = name;
            this.metadataMimeType = metadataMimeType;
            this.dataMimeType = dataMimeType;
        }

        public String getName() {
            return name;
        }

        public String getMetadataMimeType() {
            return metadataMimeType;
        }

        public String getDataMimeType() {
            return dataMimeType;
        }
    }
}
