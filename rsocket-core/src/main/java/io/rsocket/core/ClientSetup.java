package io.rsocket.core;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.DuplexConnection;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

abstract class ClientSetup {
  abstract Mono<Tuple2<ByteBuf, DuplexConnection>> init(DuplexConnection connection);
}

class DefaultClientSetup extends ClientSetup {

  @Override
  Mono<Tuple2<ByteBuf, DuplexConnection>> init(DuplexConnection connection) {
    return Mono.create(
        sink -> sink.onRequest(__ -> sink.success(Tuples.of(Unpooled.EMPTY_BUFFER, connection))));
  }
}

class ResumableClientSetup extends ClientSetup {

  @Override
  Mono<Tuple2<ByteBuf, DuplexConnection>> init(DuplexConnection connection) {
    return Mono.create(
        sink -> sink.onRequest(__ -> new SetupHandlingDuplexConnection(connection, sink)));
  }
}
