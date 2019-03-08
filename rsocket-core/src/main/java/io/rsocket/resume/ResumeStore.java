package io.rsocket.resume;

import io.netty.buffer.ByteBuf;
import io.rsocket.Closeable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ResumeStore extends Closeable {

  Mono<Void> saveFrames(Flux<ByteBuf> frames);

  /**
   * Release frames from tail of the store up to remote implied position
   */
  void releaseFrames(long remoteImpliedPos);

  /**
   * Stream frames from store tail to head. Returned Flux should terminate with error
   * if stream frames are not continuous
   */
  Flux<ByteBuf> resumeStream();

  /**
   * Local frame position as defined by RSocket protocol
   */
  long framePosition();

  /**
   * Implied frame position as defined by RSocket protocol
   */
  long frameImpliedPosition();

  /**
   * increment frame implied position
   */
  void resumableFrameReceived();
}
