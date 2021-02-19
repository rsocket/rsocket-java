package io.rsocket.plugins;

import io.netty.buffer.ByteBuf;
import io.rsocket.frame.FrameType;
import java.util.Arrays;
import java.util.stream.Stream;
import reactor.core.Disposable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Class used to track the RSocket requests lifecycles. The main difference and advantage of this
 * interceptor compares to {@link RSocketInterceptor} is that it allows intercepting the initial and
 * terminal phases on every individual request.
 *
 * <p><b>Note</b>, if any of the invocations will rise a runtime exception, this exception will be
 * caught and be propagated to {@link reactor.core.publisher.Operators#onErrorDropped(Throwable,
 * Context)}
 *
 * @since 1.1
 */
public interface RequestInterceptor extends Disposable {

  /**
   * Method which is being invoked on successful acceptance and start of a request.
   *
   * @param streamId used for the request
   * @param requestType of the request. Must be one of the following types {@link
   *     FrameType#REQUEST_FNF}, {@link FrameType#REQUEST_RESPONSE}, {@link
   *     FrameType#REQUEST_STREAM} or {@link FrameType#REQUEST_CHANNEL}
   * @param metadata taken from the initial frame
   */
  void onStart(int streamId, FrameType requestType, @Nullable ByteBuf metadata);

  /**
   * Method which is being invoked once a successfully accepted request is terminated. This method
   * can be invoked only after the {@link #onStart(int, FrameType, ByteBuf)} method. This method is
   * exclusive with {@link #onCancel(int, FrameType)}.
   *
   * @param streamId used by this request
   * @param requestType of the request. Must be one of the following types {@link
   *     FrameType#REQUEST_FNF}, {@link FrameType#REQUEST_RESPONSE}, {@link
   *     FrameType#REQUEST_STREAM} or {@link FrameType#REQUEST_CHANNEL}
   * @param t with which this finished has terminated. Must be one of the following signals
   */
  void onTerminate(int streamId, FrameType requestType, @Nullable Throwable t);

  /**
   * Method which is being invoked once a successfully accepted request is cancelled. This method
   * can be invoked only after the {@link #onStart(int, FrameType, ByteBuf)} method. This method is
   * exclusive with {@link #onTerminate(int, FrameType, Throwable)}.
   *
   * @param requestType of the request. Must be one of the following types {@link
   *     FrameType#REQUEST_FNF}, {@link FrameType#REQUEST_RESPONSE}, {@link
   *     FrameType#REQUEST_STREAM} or {@link FrameType#REQUEST_CHANNEL}
   * @param streamId used by this request
   */
  void onCancel(int streamId, FrameType requestType);

  /**
   * Method which is being invoked on the request rejection. This method is being called only if the
   * actual request can not be started and is called instead of the {@link #onStart(int, FrameType,
   * ByteBuf)} method. The reason for rejection can be one of the following:
   *
   * <p>
   *
   * <ul>
   *   <li>No available {@link io.rsocket.lease.Lease} on the requester or the responder sides
   *   <li>Invalid {@link io.rsocket.Payload} size or format on the Requester side, so the request
   *       is being rejected before the actual streamId is generated
   *   <li>A second subscription on the ongoing Request
   * </ul>
   *
   * @param rejectionReason exception which causes rejection of a particular request
   * @param requestType of the request. Must be one of the following types {@link
   *     FrameType#REQUEST_FNF}, {@link FrameType#REQUEST_RESPONSE}, {@link
   *     FrameType#REQUEST_STREAM} or {@link FrameType#REQUEST_CHANNEL}
   * @param metadata taken from the initial frame
   */
  void onReject(Throwable rejectionReason, FrameType requestType, @Nullable ByteBuf metadata);

  @Nullable
  static RequestInterceptor compose(RequestInterceptor... interceptors) {
    switch (interceptors.length) {
      case 0:
        return null;
      case 1:
        return new CompositeRequestInterceptor.SafeRequestInterceptor(interceptors[0]);
      default:
        return new CompositeRequestInterceptor(
            Arrays.stream(interceptors)
                .flatMap(
                    ri ->
                        ri instanceof CompositeRequestInterceptor
                            ? Arrays.stream(((CompositeRequestInterceptor) ri).requestInterceptors)
                            : ri instanceof CompositeRequestInterceptor.SafeRequestInterceptor
                                ? Stream.of(
                                    ((CompositeRequestInterceptor.SafeRequestInterceptor) ri)
                                        .requestInterceptor)
                                : Stream.of(ri))
                .toArray(RequestInterceptor[]::new));
    }
  }
}
