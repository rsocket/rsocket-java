package io.rsocket.lease;

import io.netty.buffer.ByteBuf;
import io.rsocket.frame.FrameType;
import reactor.core.publisher.SignalType;
import reactor.util.annotation.Nullable;

public interface LeaseTracker {

  void onAccept(int streamId, FrameType requestType, @Nullable ByteBuf metadata);

  void onReject(int streamId, FrameType requestType, @Nullable ByteBuf metadata);

  void onRelease(int streamId, SignalType terminalSignal);

  void onClose();
}
