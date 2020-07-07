package io.rsocket.examples.transport.tcp.lease.advanced.common;

import com.netflix.concurrency.limits.Limit;
import io.netty.buffer.ByteBuf;
import io.rsocket.frame.FrameType;
import io.rsocket.lease.RequestTracker;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.LongSupplier;
import reactor.core.publisher.SignalType;
import reactor.util.annotation.Nullable;

public class LimitBasedRequestTracker implements RequestTracker {

  final String connectionId;
  final PeriodicLeaseSender parent;
  final Limit limitAlgorithm;

  final ConcurrentMap<Integer, Integer> inFlightMap = new ConcurrentHashMap<>();
  final ConcurrentMap<Integer, Long> timeMap = new ConcurrentHashMap<>();

  final LongSupplier clock = System::nanoTime;

  public LimitBasedRequestTracker(
      String connectionId, PeriodicLeaseSender parent, Limit limitAlgorithm) {
    this.connectionId = connectionId;
    this.parent = parent;
    this.limitAlgorithm = limitAlgorithm;
  }

  @Override
  public void onStart(int streamId, FrameType requestType, @Nullable ByteBuf metadata) {
    long startTime = clock.getAsLong();

    int currentInFlight = parent.incrementInFlightAndGet();

    inFlightMap.put(streamId, currentInFlight);
    timeMap.put(streamId, startTime);
  }

  @Override
  public void onReject(int streamId, FrameType requestType, @Nullable ByteBuf metadata) {}

  @Override
  public void onEnd(int streamId, SignalType terminalSignal) {
    parent.decrementInFlight();

    Long startTime = timeMap.remove(streamId);
    Integer currentInflight = inFlightMap.remove(streamId);

    limitAlgorithm.onSample(
        startTime,
        clock.getAsLong() - startTime,
        currentInflight,
        !(terminalSignal == SignalType.ON_COMPLETE));
  }

  @Override
  public void dispose() {
    parent.remove(this);
  }
}
