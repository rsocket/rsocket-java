package io.rsocket.plugins;

import io.rsocket.RSocket;
import java.util.function.Function;

/** */
public @FunctionalInterface interface RSocketInterceptor extends Function<RSocket, RSocket> {}
