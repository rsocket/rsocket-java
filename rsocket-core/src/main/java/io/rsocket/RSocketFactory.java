package io.rsocket;

import io.rsocket.fragmentation.FragmentationDuplexConnection;
import io.rsocket.frame.SetupFrameFlyweight;
import io.rsocket.internal.ClientServerInputMultiplexer;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.util.PayloadImpl;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Creates RSocket. That's about it.
 *
 * @author Robert Roeser
 */
public interface RSocketFactory {
    /**
     * Creates a factory creates RSockets establish connections to other RSockets
     */
    static ClientRSocketFactory connect() {
        return new ClientRSocketFactory();
    }

    /**
     * Creates a factory creates RSockets that receives connections from other RSockets
     */
    static ServerRSocketFactory receive() {
        return new ServerRSocketFactory();
    }

    interface Start<T extends Closesable> {
        Mono<T> start();
    }

    interface SetupPayload<T> {
        T setupPayload(Payload payload);
    }

    interface Transport<T extends io.rsocket.transport.Transport, B extends Closesable> {
        Start<B> transport(Supplier<T> t);

        default Start<B> transport(T t) {
            return transport(() -> t);
        }
    }

    interface Acceptor<T extends io.rsocket.transport.Transport, A, B extends Closesable> {
        Transport<T, B> acceptor(Supplier<A> acceptor);

        default Transport<T, B> acceptor(A acceptor) {
            return acceptor(() -> acceptor);
        }
    }

    interface Fragmentation<R extends Acceptor<T, A, B>, T extends io.rsocket.transport.Transport, A, B extends Closesable> {
        R fragment(int mtu);
    }

    interface ErrorConsumer<R extends Acceptor<T, A, B>, T extends io.rsocket.transport.Transport, A, B extends Closesable> {
        R errorConsumer(Consumer<Throwable> errorConsumer);
    }

    interface KeepAlive<T> {
        T keepAlive();

        T keepAlive(Duration tickPeriod, Duration ackTimeout, int missedAcks);

        T keepAliveTickPeriod(Duration tickPeriod);

        T keepAliveAckTimeout(Duration ackTimeout);

        T keepAliveMissedAcks(int missedAcks);
    }

    interface MimeType<T> {
        T mimeType(String dataMineType, String metadataMimeType);

        T dataMineType(String dataMineType);

        T metadataMimeType(String metadataMimeType);
    }

    class ClientRSocketFactory implements
        KeepAlive<ClientRSocketFactory>,
        MimeType<ClientRSocketFactory>,
        Acceptor<ClientTransport, RSocket, RSocket>,
        Transport<ClientTransport, RSocket>,
        Fragmentation<ClientRSocketFactory, ClientTransport, RSocket, RSocket>,
        ErrorConsumer<ClientRSocketFactory, ClientTransport, RSocket, RSocket>,
        SetupPayload<ClientRSocketFactory> {

        private Supplier<RSocket> acceptor = () -> new AbstractRSocket() {
        };
        private Supplier<io.rsocket.transport.ClientTransport> transportClient;
        private Consumer<Throwable> errorConsumer = Throwable::printStackTrace;
        private int mtu = 0;
        private int flags = SetupFrameFlyweight.VALID_FLAGS - SetupFrameFlyweight.FLAGS_RESUME_ENABLE;

        private Payload setupPayload = PayloadImpl.EMPTY;

        private Duration tickPeriod = Duration.ZERO;
        private Duration ackTimeout = Duration.ofSeconds(30);
        private int missedAcks = 3;

        private String dataMineType = "application/binary";
        private String metadataMimeType = "application/binary";

        @Override
        public ClientRSocketFactory keepAlive() {
            tickPeriod = Duration.ofSeconds(20);
            return this;
        }

        @Override
        public ClientRSocketFactory keepAlive(Duration tickPeriod, Duration ackTimeout, int missedAcks) {
            this.tickPeriod = tickPeriod;
            this.ackTimeout = ackTimeout;
            this.missedAcks = missedAcks;
            return this;
        }

        @Override
        public ClientRSocketFactory keepAliveTickPeriod(Duration tickPeriod) {
            this.tickPeriod = tickPeriod;
            return this;
        }

        @Override
        public ClientRSocketFactory keepAliveAckTimeout(Duration ackTimeout) {
            this.ackTimeout = ackTimeout;
            return this;
        }

        @Override
        public ClientRSocketFactory keepAliveMissedAcks(int missedAcks) {
            this.missedAcks = missedAcks;
            return this;
        }

        @Override
        public ClientRSocketFactory mimeType(String dataMineType, String metadataMimeType) {
            this.dataMineType = dataMineType;
            this.metadataMimeType = metadataMimeType;
            return this;
        }

        @Override
        public ClientRSocketFactory dataMineType(String dataMineType) {
            this.dataMineType = dataMineType;
            return this;
        }

        @Override
        public ClientRSocketFactory metadataMimeType(String metadataMimeType) {
            this.metadataMimeType = metadataMimeType;
            return this;
        }

        @Override
        public Start<RSocket> transport(Supplier<io.rsocket.transport.ClientTransport> t) {
            return new ClientTransport().transport(t);
        }

        private class ClientTransport implements Transport<io.rsocket.transport.ClientTransport, RSocket> {
            @Override
            public Start<RSocket> transport(Supplier<io.rsocket.transport.ClientTransport> transportClient) {
                ClientRSocketFactory.this.transportClient = transportClient;
                return new StartClient();
            }
        }

        @Override
        public Transport<io.rsocket.transport.ClientTransport, RSocket> acceptor(Supplier<RSocket> acceptor) {
            this.acceptor = acceptor;
            return new ClientTransport();
        }

        @Override
        public ClientRSocketFactory fragment(int mtu) {
            this.mtu = mtu;
            return this;
        }

        @Override
        public ClientRSocketFactory errorConsumer(Consumer<Throwable> errorConsumer) {
            this.errorConsumer = errorConsumer;
            return this;
        }

        @Override
        public ClientRSocketFactory setupPayload(Payload payload) {
            this.setupPayload = payload;
            return this;
        }

        private class StartClient implements Start<RSocket> {
            @Override
            public Mono<RSocket> start() {
                return transportClient
                    .get()
                    .connect()
                    .then(connection -> {
                        Frame setupFrame = Frame.Setup
                            .from(
                                flags,
                                (int) ackTimeout.toMillis(),
                                (int) ackTimeout.toMillis() * missedAcks,
                                dataMineType,
                                metadataMimeType,
                                setupPayload);


                        ClientServerInputMultiplexer multiplexer;
                        if (mtu > 0) {
                            multiplexer = new ClientServerInputMultiplexer(new FragmentationDuplexConnection(connection, mtu));
                        } else {
                            multiplexer = new ClientServerInputMultiplexer(connection);
                        }

                        RSocketClient rSocketClient
                            = new RSocketClient(
                            multiplexer.asClientConnection(),
                            errorConsumer,
                            StreamIdSupplier.clientSupplier(),
                            tickPeriod,
                            ackTimeout,
                            missedAcks);

                        new RSocketServer(
                            multiplexer.asServerConnection(),
                            acceptor.get(),
                            errorConsumer);

                        return Mono
                            .create(sink -> {
                                connection
                                    .sendOne(setupFrame)
                                    .subscribe(v -> {}, sink::error, () -> sink.success(rSocketClient));
                            });
                    });
            }
        }
    }

    class ServerRSocketFactory implements
        Acceptor<ServerTransport, SocketAcceptor, Closesable>,
        Fragmentation<ServerRSocketFactory, ServerTransport, SocketAcceptor, Closesable>,
        ErrorConsumer<ServerRSocketFactory, ServerTransport, SocketAcceptor, Closesable> {

        private Supplier<SocketAcceptor> acceptor;
        private Supplier<io.rsocket.transport.ServerTransport> transportServer;
        private Consumer<Throwable> errorConsumer = Throwable::printStackTrace;
        private int mtu = 0;

        private ServerRSocketFactory() {
        }

        @Override
        public Transport<io.rsocket.transport.ServerTransport, Closesable> acceptor(Supplier<SocketAcceptor> acceptor) {
            this.acceptor = acceptor;
            return new ServerTransport();
        }

        @Override
        public ServerRSocketFactory fragment(int mtu) {
            this.mtu = mtu;
            return this;
        }

        @Override
        public ServerRSocketFactory errorConsumer(Consumer<Throwable> errorConsumer) {
            this.errorConsumer = errorConsumer;
            return this;
        }

        private class ServerTransport implements Transport<io.rsocket.transport.ServerTransport, Closesable> {
            @Override
            public Start transport(Supplier<io.rsocket.transport.ServerTransport> transportServer) {
                ServerRSocketFactory.this.transportServer = transportServer;
                return new ServerStart();
            }
        }

        private class ServerStart implements Start {
            @Override
            public Mono<Closesable> start() {
                return Mono.create(sink -> {
                    MonoProcessor<Void> subscribe = transportServer
                        .get()
                        .start(connection -> {
                            ClientServerInputMultiplexer multiplexer;
                            if (mtu > 0) {
                                multiplexer = new ClientServerInputMultiplexer(new FragmentationDuplexConnection(connection, mtu));
                            } else {
                                multiplexer = new ClientServerInputMultiplexer(connection);
                            }

                            return multiplexer
                                .asStreamZeroConnection()
                                .receive()
                                .next()
                                .then(setupFrame -> {
                                    ConnectionSetupPayload setupPayload = ConnectionSetupPayload.create(setupFrame);

                                    RSocketClient rSocketClient
                                        = new RSocketClient(
                                        multiplexer.asServerConnection(),
                                        errorConsumer,
                                        StreamIdSupplier.serverSupplier());

                                    Mono<RSocket> wrappedRSocketClient
                                        = Plugins
                                        .CLIENT_REACTIVE_SOCKET_INTERCEPTOR
                                        .apply(rSocketClient);

                                    return wrappedRSocketClient
                                        .then(sender ->
                                            acceptor
                                                .get()
                                                .accept(setupPayload, sender)
                                                .then(Plugins.SERVER_REACTIVE_SOCKET_INTERCEPTOR::apply)
                                        )
                                        .map(handler ->
                                            new RSocketServer(
                                                multiplexer.asClientConnection(),
                                                handler,
                                                errorConsumer))
                                        .doOnNext(sink::success)
                                        .then();
                                });
                        })
                        .doOnError(sink::error)
                        .subscribe();

                    sink.onDispose(subscribe::dispose);
                });
            }
        }
    }
}
