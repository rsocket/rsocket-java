package io.rsocket.plugins;

import io.netty.buffer.ByteBuf;
import io.rsocket.frame.FrameType;
import reactor.core.publisher.Operators;
import reactor.core.publisher.SignalType;
import reactor.util.context.Context;

public class SafeCompositeRequestInterceptor implements RequestInterceptor {

  final RequestInterceptor[] requestInterceptors;

  public SafeCompositeRequestInterceptor(RequestInterceptor[] requestInterceptors) {
    this.requestInterceptors = requestInterceptors;
  }

  @Override
  public void dispose() {
    final RequestInterceptor[] requestInterceptors = this.requestInterceptors;
    for (int i = 0; i < requestInterceptors.length; i++) {
      final RequestInterceptor requestInterceptor = requestInterceptors[i];
      requestInterceptor.dispose();
    }
  }

  @Override
  public void onStart(int streamId, FrameType requestType, ByteBuf metadata) {
    final RequestInterceptor[] requestInterceptors = this.requestInterceptors;
    for (int i = 0; i < requestInterceptors.length; i++) {
      final RequestInterceptor requestInterceptor = requestInterceptors[i];
      try {
        requestInterceptor.onStart(streamId, requestType, metadata);
      } catch (Throwable t) {
        Operators.onErrorDropped(t, Context.empty());
      }
    }
  }

  @Override
  public void onEnd(int streamId, SignalType terminalSignal) {
    final RequestInterceptor[] requestInterceptors = this.requestInterceptors;
    for (int i = 0; i < requestInterceptors.length; i++) {
      final RequestInterceptor requestInterceptor = requestInterceptors[i];
      try {
        requestInterceptor.onEnd(streamId, terminalSignal);
      } catch (Throwable t) {
        Operators.onErrorDropped(t, Context.empty());
      }
    }
  }

  @Override
  public void onReject(int streamId, FrameType requestType, ByteBuf metadata) {
    final RequestInterceptor[] requestInterceptors = this.requestInterceptors;
    for (int i = 0; i < requestInterceptors.length; i++) {
      final RequestInterceptor requestInterceptor = requestInterceptors[i];
      try {
        requestInterceptor.onReject(streamId, requestType, metadata);
      } catch (Throwable t) {
        Operators.onErrorDropped(t, Context.empty());
      }
    }
  }
}
