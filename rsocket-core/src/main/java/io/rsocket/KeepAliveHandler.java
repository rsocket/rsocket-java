package io.rsocket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.rsocket.frame.KeepAliveFrameFlyweight;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;

import java.time.Duration;

abstract class KeepAliveHandler implements Disposable {
  private final KeepAlive keepAlive;
  private final UnicastProcessor<ByteBuf> sent = UnicastProcessor.create();
  private final MonoProcessor<KeepAlive> timeout = MonoProcessor.create();
  private Disposable intervalDisposable;
  private volatile long lastReceivedMillis;

  private KeepAliveHandler(KeepAlive keepAlive) {
    this.keepAlive = keepAlive;
    this.lastReceivedMillis = System.currentTimeMillis();
    this.intervalDisposable =
        Flux.interval(Duration.ofMillis(keepAlive.getTickPeriod()))
            .subscribe(v -> onIntervalTick());
  }

  static KeepAliveHandler ofServer(KeepAlive keepAlive) {
    return new KeepAliveHandler.Server(keepAlive);
  }

  static KeepAliveHandler ofClient(KeepAlive keepAlive) {
    return new KeepAliveHandler.Client(keepAlive);
  }

  @Override
  public void dispose() {
    sent.onComplete();
    timeout.onComplete();
    intervalDisposable.dispose();
  }

  public void receive(ByteBuf keepAliveFrame) {
    this.lastReceivedMillis = System.currentTimeMillis();
    if (KeepAliveFrameFlyweight.respondFlag(keepAliveFrame)) {
      doSend(KeepAliveFrameFlyweight.encode(ByteBufAllocator.DEFAULT, false, 0,
          KeepAliveFrameFlyweight.data(keepAliveFrame).retain()));
    }
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
    if (now - lastReceivedMillis >= keepAlive.getTimeoutMillis()) {
      timeout.onNext(keepAlive);
    }
  }

  private static class Server extends KeepAliveHandler {

    Server(KeepAlive keepAlive) {
      super(keepAlive);
    }

    @Override
    void onIntervalTick() {
      doCheckTimeout();
    }
  }

  private static final class Client extends KeepAliveHandler {

    Client(KeepAlive keepAlive) {
      super(keepAlive);
    }

    @Override
    void onIntervalTick() {
      doCheckTimeout();
      doSend(KeepAliveFrameFlyweight.encode(ByteBufAllocator.DEFAULT, true, 0, Unpooled.EMPTY_BUFFER));
    }
  }

  static final class KeepAlive {
    private final long tickPeriod;
    private final long timeoutMillis;

    KeepAlive(Duration tickPeriod, Duration timeoutMillis, int maxTicks) {
      this.tickPeriod = tickPeriod.toMillis();
      this.timeoutMillis = timeoutMillis.toMillis() + maxTicks * tickPeriod.toMillis();
    }

    KeepAlive(long tickPeriod, long timeoutMillis) {
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
