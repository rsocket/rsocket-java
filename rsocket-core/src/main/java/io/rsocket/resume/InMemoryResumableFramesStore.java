/*
 * Copyright 2015-2021 the original author or authors.
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

import static io.rsocket.resume.ResumableDuplexConnection.isResumableFrame;

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.publisher.Sinks;

/**
 * writes - n (where n is frequent, primary operation) reads - m (where m == KeepAliveFrequency)
 * skip - k -> 0 (where k is the rare operation which happens after disconnection
 */
public class InMemoryResumableFramesStore extends Flux<ByteBuf>
    implements CoreSubscriber<ByteBuf>, ResumableFramesStore, Subscription {

  private static final Logger logger = LoggerFactory.getLogger(InMemoryResumableFramesStore.class);

  final Sinks.Empty<Void> disposed = Sinks.empty();
  final ArrayList<ByteBuf> cachedFrames;
  final String tag;
  final int cacheLimit;

  volatile long impliedPosition;
  static final AtomicLongFieldUpdater<InMemoryResumableFramesStore> IMPLIED_POSITION =
      AtomicLongFieldUpdater.newUpdater(InMemoryResumableFramesStore.class, "impliedPosition");

  volatile long position;
  static final AtomicLongFieldUpdater<InMemoryResumableFramesStore> POSITION =
      AtomicLongFieldUpdater.newUpdater(InMemoryResumableFramesStore.class, "position");

  volatile int cacheSize;
  static final AtomicIntegerFieldUpdater<InMemoryResumableFramesStore> CACHE_SIZE =
      AtomicIntegerFieldUpdater.newUpdater(InMemoryResumableFramesStore.class, "cacheSize");

  CoreSubscriber<? super Void> saveFramesSubscriber;

  CoreSubscriber<? super ByteBuf> actual;

  /**
   * Indicates whether there is an active connection or not.
   *
   * <ul>
   *   <li>0 - no active connection
   *   <li>1 - active connection
   *   <li>2 - disposed
   * </ul>
   *
   * <pre>
   * 0 <-----> 1
   * |         |
   * +--> 2 <--+
   * </pre>
   */
  volatile int state;

  static final AtomicIntegerFieldUpdater<InMemoryResumableFramesStore> STATE =
      AtomicIntegerFieldUpdater.newUpdater(InMemoryResumableFramesStore.class, "state");

  public InMemoryResumableFramesStore(String tag, int cacheSizeBytes) {
    this.tag = tag;
    this.cacheLimit = cacheSizeBytes;
    this.cachedFrames = new ArrayList<>();
  }

  public Mono<Void> saveFrames(Flux<ByteBuf> frames) {
    return frames
        .transform(
            Operators.lift(
                (__, actual) -> {
                  this.saveFramesSubscriber = actual;
                  return this;
                }))
        .then();
  }

  @Override
  public void releaseFrames(long remoteImpliedPos) {
    long pos = position;
    logger.debug(
        "{} Removing frames for local: {}, remote implied: {}", tag, pos, remoteImpliedPos);
    long toRemoveBytes = Math.max(0, remoteImpliedPos - pos);
    int removedBytes = 0;
    final ArrayList<ByteBuf> frames = cachedFrames;
    synchronized (this) {
      while (toRemoveBytes > removedBytes && frames.size() > 0) {
        ByteBuf cachedFrame = frames.remove(0);
        int frameSize = cachedFrame.readableBytes();
        cachedFrame.release();
        removedBytes += frameSize;
      }
    }

    if (toRemoveBytes > removedBytes) {
      throw new IllegalStateException(
          String.format(
              "Local and remote state disagreement: "
                  + "need to remove additional %d bytes, but cache is empty",
              toRemoveBytes));
    } else if (toRemoveBytes < removedBytes) {
      throw new IllegalStateException(
          "Local and remote state disagreement: local and remote frame sizes are not equal");
    } else {
      POSITION.addAndGet(this, removedBytes);
      if (cacheLimit != Integer.MAX_VALUE) {
        CACHE_SIZE.addAndGet(this, -removedBytes);
        logger.debug("{} Removed frames. Current cache size: {}", tag, cacheSize);
      }
    }
  }

  @Override
  public Flux<ByteBuf> resumeStream() {
    return this;
  }

  @Override
  public long framePosition() {
    return position;
  }

  @Override
  public long frameImpliedPosition() {
    return impliedPosition & Long.MAX_VALUE;
  }

  @Override
  public boolean resumableFrameReceived(ByteBuf frame) {
    final int frameSize = frame.readableBytes();
    for (; ; ) {
      final long impliedPosition = this.impliedPosition;

      if (impliedPosition < 0) {
        return false;
      }

      if (IMPLIED_POSITION.compareAndSet(this, impliedPosition, impliedPosition + frameSize)) {
        return true;
      }
    }
  }

  void pauseImplied() {
    for (; ; ) {
      final long impliedPosition = this.impliedPosition;

      if (IMPLIED_POSITION.compareAndSet(this, impliedPosition, impliedPosition | Long.MIN_VALUE)) {
        logger.debug("Tag {}. Paused at position[{}]", tag, impliedPosition);
        return;
      }
    }
  }

  void resumeImplied() {
    for (; ; ) {
      final long impliedPosition = this.impliedPosition;

      final long restoredImpliedPosition = impliedPosition & Long.MAX_VALUE;
      if (IMPLIED_POSITION.compareAndSet(this, impliedPosition, restoredImpliedPosition)) {
        logger.debug("Tag {}. Resumed at position[{}]", tag, restoredImpliedPosition);
        return;
      }
    }
  }

  @Override
  public Mono<Void> onClose() {
    return disposed.asMono();
  }

  @Override
  public void dispose() {
    if (STATE.getAndSet(this, 2) != 2) {
      cacheSize = 0;
      synchronized (this) {
        logger.debug("Tag {}.Disposing InMemoryFrameStore", tag);
        for (ByteBuf frame : cachedFrames) {
          if (frame != null) {
            frame.release();
          }
        }
        cachedFrames.clear();
      }
      disposed.tryEmitEmpty();
    }
  }

  @Override
  public boolean isDisposed() {
    return state == 2;
  }

  @Override
  public void onSubscribe(Subscription s) {
    saveFramesSubscriber.onSubscribe(Operators.emptySubscription());
    s.request(Long.MAX_VALUE);
  }

  @Override
  public void onError(Throwable t) {
    saveFramesSubscriber.onError(t);
  }

  @Override
  public void onComplete() {
    saveFramesSubscriber.onComplete();
  }

  @Override
  public void onNext(ByteBuf frame) {
    final int state;
    final boolean isResumable = isResumableFrame(frame);
    boolean canBeStore = isResumable;
    if (isResumable) {
      final ArrayList<ByteBuf> frames = cachedFrames;
      final int incomingFrameSize = frame.readableBytes();
      final int cacheLimit = this.cacheLimit;

      if (cacheLimit != Integer.MAX_VALUE) {
        long availableSize = cacheLimit - cacheSize;
        if (availableSize < incomingFrameSize) {
          int removedBytes = 0;
          synchronized (this) {
            while (availableSize < incomingFrameSize) {
              if (frames.size() == 0) {
                break;
              }
              ByteBuf cachedFrame;
              cachedFrame = frames.remove(0);
              final int frameSize = cachedFrame.readableBytes();
              availableSize += frameSize;
              removedBytes += frameSize;
              cachedFrame.release();
            }
          }
          CACHE_SIZE.addAndGet(this, -removedBytes);

          canBeStore = availableSize >= incomingFrameSize;
          POSITION.addAndGet(this, removedBytes + (canBeStore ? 0 : incomingFrameSize));
        } else {
          canBeStore = true;
        }
      } else {
        canBeStore = true;
      }

      state = this.state;
      if (canBeStore) {
        synchronized (this) {
          if (state != 2) {
            frames.add(frame);
          }
        }

        if (cacheLimit != Integer.MAX_VALUE) {
          CACHE_SIZE.addAndGet(this, incomingFrameSize);
        }
      }
    } else {
      state = this.state;
    }

    final CoreSubscriber<? super ByteBuf> actual = this.actual;
    if (state == 1) {
      actual.onNext(isResumable && canBeStore ? frame.retainedSlice() : frame);
    } else if (!isResumable || !canBeStore || state == 2) {
      frame.release();
    }
  }

  @Override
  public void request(long n) {}

  @Override
  public void cancel() {
    pauseImplied();
    state = 0;
  }

  @Override
  public void subscribe(CoreSubscriber<? super ByteBuf> actual) {
    final int state = this.state;
    if (state != 2) {
      resumeImplied();
      logger.debug(
          "Tag: {}. Subscribed at Position[{}] and ImpliedPosition[{}]",
          tag,
          position,
          impliedPosition);
      actual.onSubscribe(this);
      synchronized (this) {
        for (final ByteBuf frame : cachedFrames) {
          actual.onNext(frame.retainedSlice());
        }
      }

      this.actual = actual;
      STATE.compareAndSet(this, 0, 1);
    } else {
      Operators.complete(actual);
    }
  }
}
