package io.rsocket.resume;

import io.netty.buffer.ByteBuf;
import io.rsocket.Closeable;
import reactor.core.publisher.Flux;

public interface ResumeStore extends Closeable {

  /**
   * Save frame to head of the store. It is an error
   * to block current thread or throw exception in this method
   */
  void saveFrame(ByteBuf frame);

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
