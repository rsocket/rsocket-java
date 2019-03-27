package io.rsocket.internal;

import io.rsocket.DuplexConnection;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

public abstract class BaseDuplexConnection implements DuplexConnection {
  private MonoProcessor<Void> onClose = MonoProcessor.create();

  public BaseDuplexConnection() {
    onClose.doFinally(s -> doOnClose()).subscribe();
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
