package io.rsocket.plugins;

import io.rsocket.DuplexConnection;
import java.util.function.BiFunction;

/** */
public @FunctionalInterface interface DuplexConnectionInterceptor
    extends BiFunction<DuplexConnectionInterceptor.Type, DuplexConnection, DuplexConnection> {
  enum Type {
    STREAM_ZERO,
    CLIENT,
    SERVER,
    SOURCE
  }
}
