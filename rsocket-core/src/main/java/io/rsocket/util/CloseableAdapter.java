package io.rsocket.util;

import io.rsocket.Closeable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

public class CloseableAdapter implements Closeable {
  private final MonoProcessor<Void> onClose = MonoProcessor.create();
  private Runnable closeFunction;

  public CloseableAdapter(Runnable closeFunction) {
    this.closeFunction = closeFunction;
  }

  @Override
  public Mono<Void> close() {
    return Mono.defer(
        () -> {
          closeFunction.run();
          onClose.onComplete();
          return onClose;
        });
  }

  @Override
  public Mono<Void> onClose() {
    return onClose;
  }
}
