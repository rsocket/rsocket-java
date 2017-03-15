package io.reactivesocket;

import io.reactivesocket.internal.ClientServerInputMultiplexer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Function;

/**
 *
 */
public class Plugins {

    public interface FrameInterceptor extends Function<ClientServerInputMultiplexer.Type, Function<Frame, Frame>> {}
    public interface AsyncFrameInterceptor extends Function<ClientServerInputMultiplexer.Type, Function<Frame, Flux<Frame>>> {}
    public interface ReactiveSocketInterceptor extends Function<ReactiveSocket, Mono<ReactiveSocket>> {}

    public static final FrameInterceptor NOOP_FRAME_INTERCEPTOR = type -> Function.identity();
    public static final AsyncFrameInterceptor NOOP_ASYNC_FRAME_INTERCEPTOR = type -> Flux::just;
    private static final ReactiveSocketInterceptor NOOP_INTERCEPTOR = Mono::just;

    public static volatile FrameInterceptor FRAME_INTERCEPTOR = NOOP_FRAME_INTERCEPTOR;
    public static volatile AsyncFrameInterceptor ASYNC_FRAME_INTERCEPTOR = NOOP_ASYNC_FRAME_INTERCEPTOR;
    public static volatile ReactiveSocketInterceptor CLIENT_REACTIVE_SOCKET_INTERCEPTOR = NOOP_INTERCEPTOR;
    public static volatile ReactiveSocketInterceptor SERVER_REACTIVE_SOCKET_INTERCEPTOR = NOOP_INTERCEPTOR;

    private Plugins() {}

}
