package io.rsocket.util;

import io.netty.buffer.ByteBuf;
import io.rsocket.internal.ClientServerInputMultiplexer;
import java.util.function.Function;
import reactor.core.publisher.Mono;

public interface ServerSetup {
  /*accept connection as SETUP*/
  Mono<Void> setup(
      ByteBuf frame,
      ClientServerInputMultiplexer multiplexer,
      Function<ClientServerInputMultiplexer, Mono<Void>> then);

  /*accept connection as RESUME*/
  Mono<Void> resume(ByteBuf frame, ClientServerInputMultiplexer multiplexer);

  /*get KEEP-ALIVE timings based on start frame: SETUP (directly) /RESUME (lookup by resume token)*/
  Function<ByteBuf, Mono<KeepAliveData>> keepAliveData();

  default void dispose() {}
}
