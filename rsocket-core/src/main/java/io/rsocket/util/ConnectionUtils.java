package io.rsocket.util;

import io.netty.buffer.ByteBufAllocator;
import io.rsocket.frame.ErrorFrameFlyweight;
import io.rsocket.internal.ClientServerInputMultiplexer;
import reactor.core.publisher.Mono;

public class ConnectionUtils {

    public static Mono<Void> sendError(
            ByteBufAllocator allocator,
            ClientServerInputMultiplexer multiplexer,
            Exception exception) {
        return multiplexer
                .asSetupConnection()
                .sendOne(ErrorFrameFlyweight.encode(allocator, 0, exception))
                .onErrorResume(err -> Mono.empty());
    }
}
