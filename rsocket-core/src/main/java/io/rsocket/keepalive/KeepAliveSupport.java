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
import io.rsocket.frame.KeepAliveFrameCodec;
import io.rsocket.resume.ResumeStateHolder;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public abstract class KeepAliveSupport implements KeepAliveFramesAcceptor {

  final ByteBufAllocator allocator;
  final Scheduler scheduler;
  final Duration keepAliveInterval;
  final Duration keepAliveTimeout;
  final long keepAliveTimeoutMillis;

  final AtomicBoolean started = new AtomicBoolean();

  volatile Consumer<KeepAlive> onTimeout;
  volatile Consumer<ByteBuf> onFrameSent;
  volatile Disposable ticksDisposable;

  volatile ResumeStateHolder resumeStateHolder;
  volatile long lastReceivedMillis;

  private KeepAliveSupport(
      ByteBufAllocator allocator, int keepAliveInterval, int keepAliveTimeout) {
    this.allocator = allocator;
    this.scheduler = Schedulers.parallel();
    this.keepAliveInterval = Duration.ofMillis(keepAliveInterval);
    this.keepAliveTimeout = Duration.ofMillis(keepAliveTimeout);
    this.keepAliveTimeoutMillis = keepAliveTimeout;
  }

  public KeepAliveSupport start() {
    this.lastReceivedMillis = scheduler.now(TimeUnit.MILLISECONDS);
    if (started.compareAndSet(false, true)) {
      ticksDisposable =
          Flux.interval(keepAliveInterval, scheduler).subscribe(v -> onIntervalTick());
    }
    return this;
  }

  public void stop() {
    if (started.compareAndSet(true, false)) {
      ticksDisposable.dispose();
    }
  }

  @Override
  public void receive(ByteBuf keepAliveFrame) {
    this.lastReceivedMillis = scheduler.now(TimeUnit.MILLISECONDS);
    if (resumeStateHolder != null) {
      long remoteLastReceivedPos = remoteLastReceivedPosition(keepAliveFrame);
      resumeStateHolder.onImpliedPosition(remoteLastReceivedPos);
    }
    if (KeepAliveFrameCodec.respondFlag(keepAliveFrame)) {
      long localLastReceivedPos = localLastReceivedPosition();
      send(
          KeepAliveFrameCodec.encode(
              allocator,
              false,
              localLastReceivedPos,
              KeepAliveFrameCodec.data(keepAliveFrame).retain()));
    }
  }

  public KeepAliveSupport resumeState(ResumeStateHolder resumeStateHolder) {
    this.resumeStateHolder = resumeStateHolder;
    return this;
  }

  public KeepAliveSupport onSendKeepAliveFrame(Consumer<ByteBuf> onFrameSent) {
    this.onFrameSent = onFrameSent;
    return this;
  }

  public KeepAliveSupport onTimeout(Consumer<KeepAlive> onTimeout) {
    this.onTimeout = onTimeout;
    return this;
  }

  abstract void onIntervalTick();

  void send(ByteBuf frame) {
    if (onFrameSent != null) {
      onFrameSent.accept(frame);
    }
  }

  void tryTimeout() {
    long now = scheduler.now(TimeUnit.MILLISECONDS);
    if (now - lastReceivedMillis >= keepAliveTimeoutMillis) {
      if (onTimeout != null) {
        onTimeout.accept(new KeepAlive(keepAliveInterval, keepAliveTimeout));
      }
      stop();
    }
  }

  long localLastReceivedPosition() {
    return resumeStateHolder != null ? resumeStateHolder.impliedPosition() : 0;
  }

  long remoteLastReceivedPosition(ByteBuf keepAliveFrame) {
    return KeepAliveFrameCodec.lastPosition(keepAliveFrame);
  }

  @Override
  public void dispose() {
    stop();
  }

  @Override
  public boolean isDisposed() {
    return ticksDisposable.isDisposed();
  }

  public static final class ClientKeepAliveSupport extends KeepAliveSupport {

    public ClientKeepAliveSupport(
        ByteBufAllocator allocator, int keepAliveInterval, int keepAliveTimeout) {
      super(allocator, keepAliveInterval, keepAliveTimeout);
    }

    @Override
    void onIntervalTick() {
      tryTimeout();
      send(
          KeepAliveFrameCodec.encode(
              allocator, true, localLastReceivedPosition(), Unpooled.EMPTY_BUFFER));
    }
  }

  public static final class KeepAlive {
    private final Duration tickPeriod;
    private final Duration timeoutMillis;

    public KeepAlive(Duration tickPeriod, Duration timeoutMillis) {
      this.tickPeriod = tickPeriod;
      this.timeoutMillis = timeoutMillis;
    }

    public Duration getTickPeriod() {
      return tickPeriod;
    }

    public Duration getTimeout() {
      return timeoutMillis;
    }
  }
}
