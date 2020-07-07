package io.rsocket.lease;

import io.netty.buffer.ByteBuf;
import io.rsocket.frame.FrameType;
import reactor.core.Disposable;
import reactor.core.publisher.SignalType;
import reactor.util.annotation.Nullable;

/**
 * Class used to track received by the Responder RSocket requests lifecycles during lease-enabled
 * RSocket communication.
 */
public interface RequestTracker extends Disposable {

  /**
   * Method which is being invoked on successful acceptance and start of a request
   *
   * @param streamId used for the request
   * @param requestType of the request. Must be one of the following types {@link
   *     FrameType#REQUEST_FNF}, {@link FrameType#REQUEST_RESPONSE}, {@link
   *     FrameType#REQUEST_STREAM} or {@link FrameType#REQUEST_CHANNEL}
   * @param metadata provided in the request frame
   */
  void onStart(int streamId, FrameType requestType, @Nullable ByteBuf metadata);

  /**
   * Method which is being invoked once a successfully accepted request is terminated
   *
   * @param streamId used by this request
   * @param terminalSignal with which this finished has terminated. Must be one of the following
   *     signals
   */
  void onEnd(int streamId, SignalType terminalSignal);

  /**
   * Method which is being invoked on the request rejection.
   *
   * @param streamId used for the request
   * @param requestType of the request. Must be one of the following types {@link
   *     FrameType#REQUEST_FNF}, {@link FrameType#REQUEST_RESPONSE}, {@link
   *     FrameType#REQUEST_STREAM} or {@link FrameType#REQUEST_CHANNEL}
   * @param metadata provided in the request frame
   */
  void onReject(int streamId, FrameType requestType, @Nullable ByteBuf metadata);
}
