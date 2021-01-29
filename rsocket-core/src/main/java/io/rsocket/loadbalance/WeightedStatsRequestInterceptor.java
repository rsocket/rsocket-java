package io.rsocket.loadbalance;

import io.netty.buffer.ByteBuf;
import io.rsocket.RSocket;
import io.rsocket.frame.FrameType;
import io.rsocket.plugins.RequestInterceptor;
import io.rsocket.util.RSocketProxy;
import reactor.util.annotation.Nullable;

/**
 * A {@link RequestInterceptor} implementation
 *
 * @since 1.1
 */
public class WeightedStatsRequestInterceptor extends BaseWeightedStats
    implements RequestInterceptor {

  final Int2LongHashMap requestsStartTime = new Int2LongHashMap(-1);

  public WeightedStatsRequestInterceptor() {
    super();
  }

  @Override
  public final void onStart(int streamId, FrameType requestType, @Nullable ByteBuf metadata) {
    switch (requestType) {
      case REQUEST_FNF:
      case REQUEST_RESPONSE:
        final long startTime = startRequest();
        final Int2LongHashMap requestsStartTime = this.requestsStartTime;
        synchronized (requestsStartTime) {
          requestsStartTime.put(streamId, startTime);
        }
        break;
      case REQUEST_STREAM:
      case REQUEST_CHANNEL:
        this.startStream();
    }
  }

  @Override
  public final void onTerminate(int streamId, FrameType requestType, @Nullable Throwable t) {
    switch (requestType) {
      case REQUEST_FNF:
      case REQUEST_RESPONSE:
        long startTime;
        final Int2LongHashMap requestsStartTime = this.requestsStartTime;
        synchronized (requestsStartTime) {
          startTime = requestsStartTime.remove(streamId);
        }
        long endTime = stopRequest(startTime);
        if (t == null) {
          record(endTime - startTime);
        }
        break;
      case REQUEST_STREAM:
      case REQUEST_CHANNEL:
        stopStream();
        break;
    }

    if (t != null) {
      updateAvailability(0.0d);
    } else {
      updateAvailability(1.0d);
    }
  }

  @Override
  public final void onCancel(int streamId, FrameType requestType) {
    switch (requestType) {
      case REQUEST_FNF:
      case REQUEST_RESPONSE:
        long startTime;
        final Int2LongHashMap requestsStartTime = this.requestsStartTime;
        synchronized (requestsStartTime) {
          startTime = requestsStartTime.remove(streamId);
        }
        stopRequest(startTime);
        break;
      case REQUEST_STREAM:
      case REQUEST_CHANNEL:
        stopStream();
        break;
    }
  }

  @Override
  public final void onReject(Throwable rejectionReason, FrameType requestType, ByteBuf metadata) {}

  @Override
  public void dispose() {}

  /**
   * Wraps an RSocket with a proxy that implements WeightedStats
   * @param rSocket the RSocket to proxy.
   * @return the wrapped RSocket.
   */
  public RSocket wrap(RSocket rSocket) {
    return new WeightedStatsAwareRSocket(rSocket);
  }

  private class WeightedStatsAwareRSocket extends RSocketProxy implements WeightedStats {

    public WeightedStatsAwareRSocket(RSocket source) {
      super(source);
    }

    @Override
    public double higherQuantileLatency() {
      return WeightedStatsRequestInterceptor.this.higherQuantileLatency();
    }

    @Override
    public double lowerQuantileLatency() {
      return WeightedStatsRequestInterceptor.this.lowerQuantileLatency();
    }

    @Override
    public int pending() {
      return WeightedStatsRequestInterceptor.this.pending();
    }

    @Override
    public double predictedLatency() {
      return WeightedStatsRequestInterceptor.this.predictedLatency();
    }

    @Override
    public double weightedAvailability() {
      return WeightedStatsRequestInterceptor.this.weightedAvailability();
    }

  }
}
