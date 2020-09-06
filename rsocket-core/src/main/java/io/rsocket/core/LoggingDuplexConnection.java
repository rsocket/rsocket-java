package io.rsocket.core;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.DuplexConnection;
import io.rsocket.RSocketErrorException;
import io.rsocket.frame.FrameUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class LoggingDuplexConnection implements DuplexConnection {

  private static final Logger LOGGER = LoggerFactory.getLogger("io.rsocket.FrameLogger");

  final DuplexConnection source;

  LoggingDuplexConnection(DuplexConnection source) {
    this.source = source;
  }

  @Override
  public void dispose() {
    source.dispose();
  }

  @Override
  public Mono<Void> onClose() {
    return source.onClose();
  }

  @Override
  public void sendFrame(int streamId, ByteBuf frame) {
    LOGGER.debug("sending -> " + FrameUtil.toString(frame));

    source.sendFrame(streamId, frame);
  }

  @Override
  public void sendErrorAndClose(RSocketErrorException e) {
    LOGGER.debug("sending -> " + e.getClass().getSimpleName() + ": " + e.getMessage());

    source.sendErrorAndClose(e);
  }

  @Override
  public Flux<ByteBuf> receive() {
    return source
        .receive()
        .doOnNext(frame -> LOGGER.debug("receiving -> " + FrameUtil.toString(frame)));
  }

  @Override
  public ByteBufAllocator alloc() {
    return source.alloc();
  }

  static DuplexConnection wrapIfEnabled(DuplexConnection source) {
    if (LOGGER.isDebugEnabled()) {
      return new LoggingDuplexConnection(source);
    }

    return source;
  }
}
