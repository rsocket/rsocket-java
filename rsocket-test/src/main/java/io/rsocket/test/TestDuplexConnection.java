package io.rsocket.test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.DuplexConnection;
import io.rsocket.RSocketErrorException;
import io.rsocket.frame.PayloadFrameCodec;
import java.net.SocketAddress;
import java.util.function.BiFunction;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.publisher.Sinks;
import reactor.util.annotation.Nullable;

public class TestDuplexConnection implements DuplexConnection {

  final ByteBufAllocator allocator;
  final Sinks.Many<ByteBuf> inbound = Sinks.unsafe().many().unicast().onBackpressureError();
  final Sinks.Many<ByteBuf> outbound = Sinks.unsafe().many().unicast().onBackpressureError();
  final Sinks.One<Void> close = Sinks.one();

  public TestDuplexConnection(
      CoreSubscriber<? super ByteBuf> outboundSubscriber, boolean trackLeaks) {
    this.outbound.asFlux().subscribe(outboundSubscriber);
    this.allocator =
        trackLeaks
            ? LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT)
            : ByteBufAllocator.DEFAULT;
  }

  @Override
  public void dispose() {
    this.inbound.tryEmitComplete();
    this.outbound.tryEmitComplete();
    this.close.tryEmitEmpty();
  }

  @Override
  public Mono<Void> onClose() {
    return this.close.asMono();
  }

  @Override
  public void sendErrorAndClose(RSocketErrorException errorException) {}

  @Override
  public Flux<ByteBuf> receive() {
    return this.inbound
        .asFlux()
        .transform(
            Operators.lift(
                (BiFunction<
                        Scannable,
                        CoreSubscriber<? super ByteBuf>,
                        CoreSubscriber<? super ByteBuf>>)
                    ByteBufReleaserOperator::create));
  }

  @Override
  public ByteBufAllocator alloc() {
    return this.allocator;
  }

  @Override
  public SocketAddress remoteAddress() {
    return new SocketAddress() {
      @Override
      public String toString() {
        return "Test";
      }
    };
  }

  @Override
  public void sendFrame(int streamId, ByteBuf frame) {
    this.outbound.tryEmitNext(frame);
  }

  public void sendPayloadFrame(
      int streamId, ByteBuf data, @Nullable ByteBuf metadata, boolean complete) {
    sendFrame(
        streamId,
        PayloadFrameCodec.encode(this.allocator, streamId, false, complete, true, metadata, data));
  }

  static class ByteBufReleaserOperator
      implements CoreSubscriber<ByteBuf>, Subscription, Fuseable.QueueSubscription<ByteBuf> {

    static CoreSubscriber<? super ByteBuf> create(
        Scannable scannable, CoreSubscriber<? super ByteBuf> actual) {
      return new ByteBufReleaserOperator(actual);
    }

    final CoreSubscriber<? super ByteBuf> actual;

    Subscription s;

    public ByteBufReleaserOperator(CoreSubscriber<? super ByteBuf> actual) {
      this.actual = actual;
    }

    @Override
    public void onSubscribe(Subscription s) {
      if (Operators.validate(this.s, s)) {
        this.s = s;
        this.actual.onSubscribe(this);
      }
    }

    @Override
    public void onNext(ByteBuf buf) {
      this.actual.onNext(buf);
      buf.release();
    }

    @Override
    public void onError(Throwable t) {
      actual.onError(t);
    }

    @Override
    public void onComplete() {
      actual.onComplete();
    }

    @Override
    public void request(long n) {
      s.request(n);
    }

    @Override
    public void cancel() {
      s.cancel();
    }

    @Override
    public int requestFusion(int requestedMode) {
      return Fuseable.NONE;
    }

    @Override
    public ByteBuf poll() {
      throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
    }

    @Override
    public int size() {
      throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
    }

    @Override
    public boolean isEmpty() {
      throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
    }
  }
}
