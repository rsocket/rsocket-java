package io.reactivesocket;

import org.reactivestreams.Publisher;

import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

@FunctionalInterface
public interface ReactiveSocketFactory {
    Publisher<ReactiveSocket> call(SocketAddress address, long timeout, TimeUnit timeUnit);
}
