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

package io.rsocket.resume;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.DuplexConnection;
import io.rsocket.RSocketErrorException;
import io.rsocket.frame.FrameHeaderCodec;
import io.rsocket.internal.UnboundedProcessor;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

public class ResumableDuplexConnection extends Flux<ByteBuf>
    implements DuplexConnection, CoreSubscriber<ByteBuf>, Subscription {

  final ResumableFramesStore resumableFramesStore;

  final UnboundedProcessor<ByteBuf> savableFramesSender;
  final Disposable framesSaverDisposable;
  final MonoProcessor<Void> onClose;

  CoreSubscriber<? super ByteBuf> receiveSubscriber;

  volatile int state;
  static final AtomicIntegerFieldUpdater<ResumableDuplexConnection> STATE =
      AtomicIntegerFieldUpdater.newUpdater(ResumableDuplexConnection.class, "state");

  volatile DuplexConnection activeConnection;
  static final AtomicReferenceFieldUpdater<ResumableDuplexConnection, DuplexConnection>
      ACTIVE_CONNECTION =
          AtomicReferenceFieldUpdater.newUpdater(
              ResumableDuplexConnection.class, DuplexConnection.class, "activeConnection");

  public ResumableDuplexConnection(
      DuplexConnection initialConnection, ResumableFramesStore resumableFramesStore) {
    this.resumableFramesStore = resumableFramesStore;
    this.savableFramesSender = new UnboundedProcessor<>();
    this.framesSaverDisposable = resumableFramesStore.saveFrames(savableFramesSender).subscribe();
    this.onClose = MonoProcessor.create();

    resumableFramesStore
        .resumeStream()
        .takeUntilOther(initialConnection.onClose())
        .subscribe(f -> initialConnection.sendFrame(FrameHeaderCodec.streamId(f), f));

    ACTIVE_CONNECTION.lazySet(this, initialConnection);
  }

  public boolean connect(DuplexConnection nextConnection) {
    final DuplexConnection activeConnection = this.activeConnection;
    if (activeConnection != DisposedConnection.INSTANCE
        && ACTIVE_CONNECTION.compareAndSet(this, activeConnection, nextConnection)) {

      activeConnection.dispose();
      nextConnection.receive().subscribe(this);
      resumableFramesStore
          .resumeStream()
          .takeUntilOther(nextConnection.onClose())
          .subscribe(f -> nextConnection.sendFrame(FrameHeaderCodec.streamId(f), f));
      return true;
    } else {
      return false;
    }
  }

  public void disconnect() {
    final DuplexConnection activeConnection = this.activeConnection;
    if (activeConnection != DisposedConnection.INSTANCE) {
      activeConnection.dispose();
    }
  }

  @Override
  public void sendFrame(int streamId, ByteBuf frame) {
    if (streamId == 0) {
      savableFramesSender.onNextPrioritized(frame);
    } else {
      savableFramesSender.onNext(frame);
    }
  }

  @Override
  public void sendErrorAndClose(RSocketErrorException rSocketErrorException) {
    final DuplexConnection activeConnection =
        ACTIVE_CONNECTION.getAndSet(this, DisposedConnection.INSTANCE);
    if (activeConnection == DisposedConnection.INSTANCE) {
      return;
    }

    activeConnection.sendErrorAndClose(rSocketErrorException);
    activeConnection
        .onClose()
        .subscribe(
            null,
            t -> {
              onClose.onError(t);
              framesSaverDisposable.dispose();
            },
            () -> {
              final Throwable cause = rSocketErrorException.getCause();
              if (cause == null) {
                onClose.onComplete();
              } else {
                onClose.onError(cause);
              }
              framesSaverDisposable.dispose();
            });
  }

  @Override
  public Flux<ByteBuf> receive() {
    return this;
  }

  @Override
  public ByteBufAllocator alloc() {
    return activeConnection.alloc();
  }

  @Override
  public Mono<Void> onClose() {
    return onClose;
  }

  @Override
  public void dispose() {
    final DuplexConnection activeConnection =
        ACTIVE_CONNECTION.getAndSet(this, DisposedConnection.INSTANCE);
    if (activeConnection == DisposedConnection.INSTANCE) {
      return;
    }

    if (activeConnection != null) {
      activeConnection.dispose();
    }

    onClose.onComplete();
    framesSaverDisposable.dispose();
  }

  @Override
  public boolean isDisposed() {
    return onClose.isDisposed();
  }

  @Override
  public void onSubscribe(Subscription s) {
    s.request(Long.MAX_VALUE);
  }

  @Override
  public void onNext(ByteBuf frame) {
    if (isResumableFrame(frame)) {
      resumableFramesStore.resumableFrameReceived(frame);
    }

    receiveSubscriber.onNext(frame);
  }

  static boolean isResumableFrame(ByteBuf frame) {
    switch (FrameHeaderCodec.nativeFrameType(frame)) {
      case REQUEST_CHANNEL:
      case REQUEST_STREAM:
      case REQUEST_RESPONSE:
      case REQUEST_FNF:
      case REQUEST_N:
      case CANCEL:
      case ERROR:
      case PAYLOAD:
        return true;
      default:
        return false;
    }
  }

  @Override
  public void onError(Throwable t) {}

  @Override
  public void onComplete() {}

  @Override
  public void request(long n) {
    if (state == 1 && STATE.compareAndSet(this, 1, 2)) {
      final DuplexConnection connection = this.activeConnection;
      if (connection != null) {
        connection.receive().subscribe(this);
      }
    }
  }

  @Override
  public void cancel() {
    dispose();
  }

  @Override
  public void subscribe(CoreSubscriber<? super ByteBuf> receiverSubscriber) {
    if (state == 0 && STATE.compareAndSet(this, 0, 1)) {
      receiveSubscriber = receiverSubscriber;
      receiverSubscriber.onSubscribe(this);
    }
  }

  private static final class DisposedConnection implements DuplexConnection {

    static final DisposedConnection INSTANCE = new DisposedConnection();

    private DisposedConnection() {}

    @Override
    public void dispose() {}

    @Override
    public Mono<Void> onClose() {
      return Mono.never();
    }

    @Override
    public void sendFrame(int streamId, ByteBuf frame) {}

    @Override
    public Flux<ByteBuf> receive() {
      return Flux.never();
    }

    @Override
    public void sendErrorAndClose(RSocketErrorException e) {}

    @Override
    public ByteBufAllocator alloc() {
      return ByteBufAllocator.DEFAULT;
    }
  }
}
