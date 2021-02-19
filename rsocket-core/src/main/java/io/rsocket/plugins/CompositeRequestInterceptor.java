package io.rsocket.plugins;

import io.netty.buffer.ByteBuf;
import io.rsocket.RSocket;
import io.rsocket.frame.FrameType;
import java.util.List;
import java.util.function.Function;
import reactor.core.publisher.Operators;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

class CompositeRequestInterceptor implements RequestInterceptor {

  final RequestInterceptor[] requestInterceptors;

  CompositeRequestInterceptor(RequestInterceptor[] requestInterceptors) {
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
  public void onStart(int streamId, FrameType requestType, @Nullable ByteBuf metadata) {
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
  public void onTerminate(int streamId, FrameType requestType, @Nullable Throwable cause) {
    final RequestInterceptor[] requestInterceptors = this.requestInterceptors;
    for (int i = 0; i < requestInterceptors.length; i++) {
      final RequestInterceptor requestInterceptor = requestInterceptors[i];
      try {
        requestInterceptor.onTerminate(streamId, requestType, cause);
      } catch (Throwable t) {
        Operators.onErrorDropped(t, Context.empty());
      }
    }
  }

  @Override
  public void onCancel(int streamId, FrameType requestType) {
    final RequestInterceptor[] requestInterceptors = this.requestInterceptors;
    for (int i = 0; i < requestInterceptors.length; i++) {
      final RequestInterceptor requestInterceptor = requestInterceptors[i];
      try {
        requestInterceptor.onCancel(streamId, requestType);
      } catch (Throwable t) {
        Operators.onErrorDropped(t, Context.empty());
      }
    }
  }

  @Override
  public void onReject(
      Throwable rejectionReason, FrameType requestType, @Nullable ByteBuf metadata) {
    final RequestInterceptor[] requestInterceptors = this.requestInterceptors;
    for (int i = 0; i < requestInterceptors.length; i++) {
      final RequestInterceptor requestInterceptor = requestInterceptors[i];
      try {
        requestInterceptor.onReject(rejectionReason, requestType, metadata);
      } catch (Throwable t) {
        Operators.onErrorDropped(t, Context.empty());
      }
    }
  }

  @Nullable
  static RequestInterceptor create(
      RSocket rSocket, List<Function<RSocket, ? extends RequestInterceptor>> interceptors) {
    switch (interceptors.size()) {
      case 0:
        return null;
      case 1:
        return new SafeRequestInterceptor(interceptors.get(0).apply(rSocket));
      default:
        return new CompositeRequestInterceptor(
            interceptors.stream().map(f -> f.apply(rSocket)).toArray(RequestInterceptor[]::new));
    }
  }

  static class SafeRequestInterceptor implements RequestInterceptor {

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
    public void onStart(int streamId, FrameType requestType, @Nullable ByteBuf metadata) {
      try {
        requestInterceptor.onStart(streamId, requestType, metadata);
      } catch (Throwable t) {
        Operators.onErrorDropped(t, Context.empty());
      }
    }

    @Override
    public void onTerminate(int streamId, FrameType requestType, @Nullable Throwable cause) {
      try {
        requestInterceptor.onTerminate(streamId, requestType, cause);
      } catch (Throwable t) {
        Operators.onErrorDropped(t, Context.empty());
      }
    }

    @Override
    public void onCancel(int streamId, FrameType requestType) {
      try {
        requestInterceptor.onCancel(streamId, requestType);
      } catch (Throwable t) {
        Operators.onErrorDropped(t, Context.empty());
      }
    }

    @Override
    public void onReject(
        Throwable rejectionReason, FrameType requestType, @Nullable ByteBuf metadata) {
      try {
        requestInterceptor.onReject(rejectionReason, requestType, metadata);
      } catch (Throwable t) {
        Operators.onErrorDropped(t, Context.empty());
      }
    }
  }
}
