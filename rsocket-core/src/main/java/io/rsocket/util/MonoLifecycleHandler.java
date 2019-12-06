package io.rsocket.util;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import reactor.core.publisher.SignalType;

public interface MonoLifecycleHandler<T> {

  default void doOnSubscribe() {}

  /**
   * Handler which is invoked on the terminal activity within a given Monoø
   *
   * @param signalType a type of signal which explain what happened
   * @param element an carried element. May not be present if stream is empty or cancelled or
   *     errored
   * @param e an carried error. May not be present if stream is cancelled or completed successfully
   */
  default void doOnTerminal(
      @Nonnull SignalType signalType, @Nullable T element, @Nullable Throwable e) {}
}
