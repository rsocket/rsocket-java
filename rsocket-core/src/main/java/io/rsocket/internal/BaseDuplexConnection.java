package io.rsocket.internal;

import io.netty.buffer.ByteBuf;
import io.rsocket.DuplexConnection;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

public abstract class BaseDuplexConnection implements DuplexConnection {
  protected MonoProcessor<Void> onClose = MonoProcessor.create();

  @SuppressWarnings("deprecation")
  protected UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();

  public BaseDuplexConnection() {
    onClose.doFinally(s -> doOnClose()).subscribe();
  }

  @Override
  public void sendFrame(int streamId, ByteBuf frame, boolean prioritize) {
    if (prioritize) {
      sender.onNextPrioritized(frame);
    } else {
      sender.onNext(frame);
    }
  }

  protected abstract void doOnClose();

  @Override
  public final Mono<Void> onClose() {
    return onClose;
  }

  @Override
  public final void dispose() {
    onClose.onComplete();
  }

  @Override
  public final boolean isDisposed() {
    return onClose.isDisposed();
  }
}
