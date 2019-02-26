package io.rsocket.keepalive;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.rsocket.frame.KeepAliveFrameFlyweight;
import io.rsocket.resume.ResumeStateHolder;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

abstract class KeepAliveHandler implements Disposable {
  protected final ByteBufAllocator allocator;
  private final Duration keepAlivePeriod;
  private final Duration keepAliveTimeout;
  private volatile Optional<ResumeStateHolder> resumeStateHolder = Optional.empty();
  private final UnicastProcessor<ByteBuf> sent = UnicastProcessor.create();
  private final MonoProcessor<KeepAlive> timeout = MonoProcessor.create();
  private final AtomicReference<Disposable> intervalDisposable = new AtomicReference<>();
  private volatile long lastReceivedMillis;

  static KeepAliveHandler ofServer(ByteBufAllocator allocator,
                                   Duration keepAlivePeriod,
                                   Duration keepAliveTimeout) {
    return new KeepAliveHandler.Server(allocator, keepAlivePeriod, keepAliveTimeout);
  }

  static KeepAliveHandler ofClient(ByteBufAllocator allocator,
                                   Duration keepAlivePeriod,
                                   Duration keepAliveTimeout) {
    return new KeepAliveHandler.Client(allocator, keepAlivePeriod, keepAliveTimeout);
  }

  private KeepAliveHandler(ByteBufAllocator allocator,
                           Duration keepAlivePeriod,
                           Duration keepAliveTimeout) {
    this.allocator = allocator;
    this.keepAlivePeriod = keepAlivePeriod;
    this.keepAliveTimeout = keepAliveTimeout;
  }

  public void start() {
    this.lastReceivedMillis = System.currentTimeMillis();
    intervalDisposable.compareAndSet(
        null, Flux.interval(keepAlivePeriod).subscribe(v -> onIntervalTick()));
  }

  @Override
  public void dispose() {
    Disposable d = intervalDisposable.getAndSet(Disposables.disposed());
    if (d != null) {
      d.dispose();
    }
    sent.onComplete();
    timeout.onComplete();
  }

  public long receive(ByteBuf keepAliveFrame) {
    this.lastReceivedMillis = System.currentTimeMillis();
    long remoteLastReceivedPos = KeepAliveFrameFlyweight.lastPosition(keepAliveFrame);
    if (KeepAliveFrameFlyweight.respondFlag(keepAliveFrame)) {
      long localLastReceivedPos = obtainLastReceivedPos();
      doSend(
          KeepAliveFrameFlyweight.encode(
              allocator,
              false,
              localLastReceivedPos,
              KeepAliveFrameFlyweight.data(keepAliveFrame).retain()));
    }
    return remoteLastReceivedPos;
  }

  public void resumeState(ResumeStateHolder resumeStateHolder) {
    this.resumeStateHolder = Optional.of(resumeStateHolder);
  }

  public boolean hasResumeState() {
    return resumeStateHolder.isPresent();
  }

  public Flux<ByteBuf> send() {
    return sent;
  }

  public Mono<KeepAlive> timeout() {
    return timeout;
  }

  abstract void onIntervalTick();

  void doSend(ByteBuf frame) {
    sent.onNext(frame);
  }

  void doCheckTimeout() {
    long now = System.currentTimeMillis();
    if (now - lastReceivedMillis >= keepAliveTimeout.toMillis()) {
      timeout.onNext(new KeepAlive(keepAlivePeriod.toMillis(), keepAliveTimeout.toMillis()));
    }
  }

  Long obtainLastReceivedPos() {
    return resumeStateHolder.map(ResumeStateHolder::impliedPosition).orElse(0L);
  }

  private static class Server extends KeepAliveHandler {

    Server(ByteBufAllocator allocator, Duration keepAlivePeriod, Duration keepAliveTimeout) {
      super(allocator, keepAlivePeriod, keepAliveTimeout);
    }

    @Override
    void onIntervalTick() {
      doCheckTimeout();
    }
  }

  private static final class Client extends KeepAliveHandler {

    Client(ByteBufAllocator allocator, Duration keepAlivePeriod, Duration keepAliveTimeout) {
      super(allocator, keepAlivePeriod, keepAliveTimeout);
    }

    @Override
    void onIntervalTick() {
      doCheckTimeout();
      doSend(KeepAliveFrameFlyweight.encode(
          allocator,
          true,
          obtainLastReceivedPos(),
          Unpooled.EMPTY_BUFFER));
    }
  }

  public static final class KeepAlive {
    private final long tickPeriod;
    private final long timeoutMillis;

    public KeepAlive(long tickPeriod, long timeoutMillis) {
      this.tickPeriod = tickPeriod;
      this.timeoutMillis = timeoutMillis;
    }

    public long getTickPeriod() {
      return tickPeriod;
    }

    public long getTimeoutMillis() {
      return timeoutMillis;
    }
  }
}
