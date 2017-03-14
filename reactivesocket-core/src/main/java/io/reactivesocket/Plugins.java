package io.reactivesocket;

import io.reactivesocket.internal.ClientServerInputMultiplexer;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 *
 */
public class Plugins {
    public static final FrameCounter NOOP_COUNTER = type -> frame -> {};

    private static final ReactiveSocketInterceptor NOOP_INTERCEPTOR = reactiveSocket -> reactiveSocket;

    public interface FrameCounter extends Function<ClientServerInputMultiplexer.Type, Consumer<Frame>> {}

    public interface ReactiveSocketInterceptor extends Function<ReactiveSocket, ReactiveSocket> {}

    public static volatile Plugins.FrameCounter FRAME_COUNTER = NOOP_COUNTER;

    public static volatile ReactiveSocketInterceptor CLIENT_REACTIVE_SOCKET_INTERCEPTOR = NOOP_INTERCEPTOR;

    public static volatile ReactiveSocketInterceptor SERVER_REACTIVE_SOCKET_INTERCEPTOR = NOOP_INTERCEPTOR;

    public static boolean emptyCounter() {
        return FRAME_COUNTER == NOOP_COUNTER;
    }

    public static boolean emptyClientReactiveSocketInterceptor() {
        return CLIENT_REACTIVE_SOCKET_INTERCEPTOR == NOOP_INTERCEPTOR;
    }

    public static boolean emptyServerReactiveSocketInterceptor() {
        return SERVER_REACTIVE_SOCKET_INTERCEPTOR == NOOP_INTERCEPTOR;
    }

    private Plugins() {}

}
