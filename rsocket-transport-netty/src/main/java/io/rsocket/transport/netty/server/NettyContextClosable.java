package io.rsocket.transport.netty.server;

import io.rsocket.Closeable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.ipc.netty.NettyContext;

/**
 *
 */
class NettyContextClosable implements Closeable {
    private NettyContext context;

    private MonoProcessor<Void> onClose;

    NettyContextClosable(NettyContext context) {
        this.onClose = MonoProcessor.create();
        this.context = context;
    }

    @Override
    public Mono<Void> close() {
        return Mono
            .empty()
            .doFinally(s -> {
                context.dispose();
                onClose.onComplete();
            })
            .then();
    }

    @Override
    public Mono<Void> onClose() {
        return onClose;
    }
}
