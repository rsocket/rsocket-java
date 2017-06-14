package io.rsocket.transport.netty.server;

import io.rsocket.Closeable;
import java.net.InetSocketAddress;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.ipc.netty.NettyContext;

/**
 * A {@link Closeable} wrapping a {@link NettyContext}, allowing for close and aware of its address.
 */
public class NettyContextCloseable implements Closeable {
  private NettyContext context;

  private MonoProcessor<Void> onClose;

  NettyContextCloseable(NettyContext context) {
    this.onClose = MonoProcessor.create();
    this.context = context;
  }

  @Override
  public Mono<Void> close() {
    return Mono.empty()
        .doFinally(
            s -> {
              context.dispose();
              onClose.onComplete();
            })
        .then();
  }

  @Override
  public Mono<Void> onClose() {
    return onClose;
  }

  /** @see NettyContext#address() */
  public InetSocketAddress address() {
    return context.address();
  }
}
