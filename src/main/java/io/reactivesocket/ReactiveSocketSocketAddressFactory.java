package io.reactivesocket;

import java.net.SocketAddress;

@FunctionalInterface
public interface ReactiveSocketSocketAddressFactory<R extends ReactiveSocket> extends ReactiveSocketFactory<SocketAddress, R> {

}
