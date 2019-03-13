package io.rsocket.resume;

import io.netty.buffer.ByteBuf;
import io.rsocket.Closeable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Store for resumable frames
 */
public interface ResumableFramesStore extends Closeable {

  /**
   * Save resumable frames for potential resumption
   * @param frames {@link Flux} of resumable frames
   * @return {@link Mono} which completes once all resume frames are written
   */
  Mono<Void> saveFrames(Flux<ByteBuf> frames);

  /**
   * Release frames from tail of the store up to remote implied position
   */
  void releaseFrames(long remoteImpliedPos);

  /**
   * @return {@link Flux} of frames from store tail to head.
   * It should terminate with error if frames are not continuous
   */
  Flux<ByteBuf> resumeStream();

  /**
   * @return Local frame position as defined by RSocket protocol
   */
  long framePosition();

  /**
   * @return Implied frame position as defined by RSocket protocol
   */
  long frameImpliedPosition();

  /**
   * Received resumable frame as defined by RSocket protocol.
   * Implementation must increment frame implied position
   */
  void resumableFrameReceived();
}
