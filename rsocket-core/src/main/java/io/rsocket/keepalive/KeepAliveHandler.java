/*
 * Copyright 2015-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.keepalive;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.rsocket.frame.KeepAliveFrameFlyweight;
import io.rsocket.resume.ResumeStateHolder;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.UnicastProcessor;

abstract class KeepAliveHandler implements Disposable {
  final ByteBufAllocator allocator;
  private final Duration keepAlivePeriod;
  private final long keepAliveTimeout;
  private volatile ResumeStateHolder resumeStateHolder;
  private final UnicastProcessor<ByteBuf> sent = UnicastProcessor.create();
  private final MonoProcessor<KeepAlive> timeout = MonoProcessor.create();
  private final AtomicReference<Disposable> intervalDisposable = new AtomicReference<>();
  private volatile long lastReceivedMillis;

  static KeepAliveHandler ofServer(
      ByteBufAllocator allocator, Duration keepAlivePeriod, Duration keepAliveTimeout) {
    return new KeepAliveHandler.Server(allocator, keepAlivePeriod, keepAliveTimeout);
  }

  static KeepAliveHandler ofClient(
      ByteBufAllocator allocator, Duration keepAlivePeriod, Duration keepAliveTimeout) {
    return new KeepAliveHandler.Client(allocator, keepAlivePeriod, keepAliveTimeout);
  }

  private KeepAliveHandler(
      ByteBufAllocator allocator, Duration keepAlivePeriod, Duration keepAliveTimeout) {
    this.allocator = allocator;
    this.keepAlivePeriod = keepAlivePeriod;
    this.keepAliveTimeout = keepAliveTimeout.toMillis();
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
    this.resumeStateHolder = resumeStateHolder;
  }

  public boolean hasResumeState() {
    return resumeStateHolder != null;
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
    if (now - lastReceivedMillis >= keepAliveTimeout) {
      timeout.onNext(new KeepAlive(keepAlivePeriod.toMillis(), keepAliveTimeout));
    }
  }

  long obtainLastReceivedPos() {
    return resumeStateHolder != null ? resumeStateHolder.impliedPosition() : 0;
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
      doSend(
          KeepAliveFrameFlyweight.encode(
              allocator, true, obtainLastReceivedPos(), Unpooled.EMPTY_BUFFER));
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
