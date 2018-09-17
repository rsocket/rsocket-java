package io.rsocket;

import io.netty.buffer.Unpooled;
import java.time.Duration;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;

abstract class KeepAliveHandler {
  private final KeepAlive keepAlive;
  private final UnicastProcessor<Frame> sent = UnicastProcessor.create();
  private final MonoProcessor<KeepAlive> timeout = MonoProcessor.create();
  private final Flux<Long> interval;
  private Disposable intervalDisposable;
  private volatile long lastReceivedMillis;

  static KeepAliveHandler ofServer(KeepAlive keepAlive) {
    return new KeepAliveHandler.Server(keepAlive);
  }

  static KeepAliveHandler ofClient(KeepAlive keepAlive) {
    return new KeepAliveHandler.Client(keepAlive);
  }

  private KeepAliveHandler(KeepAlive keepAlive) {
    this.keepAlive = keepAlive;
    this.interval = Flux.interval(Duration.ofMillis(keepAlive.getTickPeriod()));
  }

  public void start() {
    this.lastReceivedMillis = System.currentTimeMillis();
    intervalDisposable = interval.subscribe(v -> onIntervalTick());
  }

  public void stop() {
    sent.onComplete();
    timeout.onComplete();
    if (intervalDisposable != null) {
      intervalDisposable.dispose();
    }
  }

  public void receive(Frame keepAliveFrame) {
    this.lastReceivedMillis = System.currentTimeMillis();
    if (Frame.Keepalive.hasRespondFlag(keepAliveFrame)) {
      doSend(Frame.Keepalive.from(Unpooled.wrappedBuffer(keepAliveFrame.getData()), false));
    }
  }

  public Flux<Frame> send() {
    return sent;
  }

  public Mono<KeepAlive> timeout() {
    return timeout;
  }

  abstract void onIntervalTick();

  void doSend(Frame frame) {
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
      doSend(Frame.Keepalive.from(Unpooled.EMPTY_BUFFER, true));
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
