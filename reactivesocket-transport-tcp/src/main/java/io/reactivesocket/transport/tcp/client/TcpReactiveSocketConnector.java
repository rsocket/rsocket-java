package io.reactivesocket.transport.tcp.client;

import io.netty.buffer.ByteBuf;
import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.ReactiveSocketConnector;
import io.reactivex.netty.protocol.tcp.client.TcpClient;
import org.reactivestreams.Publisher;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Function;

public class TcpReactiveSocketConnector implements ReactiveSocketConnector<SocketAddress> {

    private final ConcurrentMap<SocketAddress, TcpReactiveSocketFactory> socketFactories;
    private final ConnectionSetupPayload setupPayload;
    private final Consumer<Throwable> errorStream;
    private final Function<SocketAddress, TcpClient<ByteBuf, ByteBuf>> clientFactory;

    private TcpReactiveSocketConnector(ConnectionSetupPayload setupPayload, Consumer<Throwable> errorStream,
                                       Function<SocketAddress, TcpClient<ByteBuf, ByteBuf>> clientFactory) {
        this.setupPayload = setupPayload;
        this.errorStream = errorStream;
        this.clientFactory = clientFactory;
        socketFactories = new ConcurrentHashMap<>();
    }

    @Override
    public Publisher<ReactiveSocket> connect(SocketAddress address) {
        return socketFactories.computeIfAbsent(address, socketAddress -> {
            TcpClient<ByteBuf, ByteBuf> client = clientFactory.apply(socketAddress);
            return TcpReactiveSocketFactory.create(socketAddress, client, setupPayload, errorStream);
        }).apply();
    }

    public static TcpReactiveSocketConnector create(ConnectionSetupPayload setupPayload,
                                                    Consumer<Throwable> errorStream) {
        return new TcpReactiveSocketConnector(setupPayload, errorStream,
                                              socketAddress -> TcpClient.newClient(socketAddress));
    }

    public static TcpReactiveSocketConnector create(ConnectionSetupPayload setupPayload,
                                                    Consumer<Throwable> errorStream,
                                                    Function<SocketAddress, TcpClient<ByteBuf, ByteBuf>> clientFactory) {
        return new TcpReactiveSocketConnector(setupPayload, errorStream, clientFactory);
    }
}
