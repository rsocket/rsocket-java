package io.rsocket.resume;

import io.netty.buffer.ByteBuf;
import io.rsocket.Closeable;
import io.rsocket.DuplexConnection;
import reactor.core.publisher.Mono;

public interface RSocketSession<T> extends Closeable {

  ResumeToken token();

  DuplexConnection resumableConnection();

  RSocketSession continueWith(T ResumeAwareConnectionFactory);

  RSocketSession resumeWith(ByteBuf resumeFrame);

  void reconnect(ResumeAwareConnection connection);

  @Override
  default Mono<Void> onClose() {
    return resumableConnection().onClose();
  }

  @Override
  default void dispose() {
    resumableConnection().dispose();
  }

  @Override
  default boolean isDisposed() {
    return resumableConnection().isDisposed();
  }
}
