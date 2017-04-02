package io.reactivesocket;

import reactor.core.publisher.Mono;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 *
 */
public class Plugins {

    public interface DuplexConnectionInterceptor extends BiFunction<DuplexConnectionInterceptor.Type, DuplexConnection, DuplexConnection> {
        enum Type { STREAM_ZERO, CLIENT, SERVER, SOURCE }
    }
    public interface ReactiveSocketInterceptor extends Function<ReactiveSocket, Mono<ReactiveSocket>> {}

    public static final DuplexConnectionInterceptor NOOP_DUPLEX_CONNECTION_INTERCEPTOR = (type, connection) -> connection;
    private static final ReactiveSocketInterceptor NOOP_INTERCEPTOR = Mono::just;

    public static volatile DuplexConnectionInterceptor DUPLEX_CONNECTION_INTERCEPTOR = NOOP_DUPLEX_CONNECTION_INTERCEPTOR;
    public static volatile ReactiveSocketInterceptor CLIENT_REACTIVE_SOCKET_INTERCEPTOR = NOOP_INTERCEPTOR;
    public static volatile ReactiveSocketInterceptor SERVER_REACTIVE_SOCKET_INTERCEPTOR = NOOP_INTERCEPTOR;

    private Plugins() {}

}
