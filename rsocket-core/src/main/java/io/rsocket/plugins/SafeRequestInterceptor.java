package io.rsocket.plugins;

import io.netty.buffer.ByteBuf;
import io.rsocket.frame.FrameType;
import reactor.core.publisher.Operators;
import reactor.core.publisher.SignalType;
import reactor.util.context.Context;

public class SafeRequestInterceptor implements RequestInterceptor {

  final RequestInterceptor requestInterceptor;

  public SafeRequestInterceptor(RequestInterceptor requestInterceptor) {
    this.requestInterceptor = requestInterceptor;
  }

  @Override
  public void dispose() {
    requestInterceptor.dispose();
  }

  @Override
  public boolean isDisposed() {
    return requestInterceptor.isDisposed();
  }

  @Override
  public void onStart(int streamId, FrameType requestType, ByteBuf metadata) {
    try {
      requestInterceptor.onStart(streamId, requestType, metadata);
    } catch (Throwable t) {
      Operators.onErrorDropped(t, Context.empty());
    }
  }

  @Override
  public void onEnd(int streamId, SignalType terminalSignal) {
    try {
      requestInterceptor.onEnd(streamId, terminalSignal);
    } catch (Throwable t) {
      Operators.onErrorDropped(t, Context.empty());
    }
  }

  @Override
  public void onReject(int streamId, FrameType requestType, ByteBuf metadata) {
    try {
      requestInterceptor.onReject(streamId, requestType, metadata);
    } catch (Throwable t) {
      Operators.onErrorDropped(t, Context.empty());
    }
  }
}
